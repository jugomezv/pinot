/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.core.data.manager.realtime;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Class to track maximum realtime delay for a given table on a given server.
 * Highlights:
 * 1-An object of this class is hosted by each RealtimeTableDataManager.
 * 2-The object tracks ingestion delays for all partitions hosted by the current server for the given Realtime table.
 * 3-Partition delays are updated by all LLRealtimeSegmentDataManager objects hosted in the corresponding
 *   RealtimeTableDataManager.
 * 4-The class tracks the maximum of all ingestion delays observed for all partitions of the given table.
 * 5-A Metric is derived from reading the maximum tracked by this class.
 * 6-Delay reported for partitions that do not have events to consume is reported as zero.
 * 7-We track the time at which each delay sample was collected so that delay can be increased when partition stops
 *   consuming for any reason other than no events being available for consumption.
 * 8-Segments that go from CONSUMING to DROPPED states stop being tracked so their delays do not cloud
 *   delays of active partitions.
 * 9-When a segment goes from CONSUMING to ONLINE, we start ticking time for the corresponding partition.
 *   If no consumption is noticed after a timeout, then we read ideal state to confirm the server still hosts the
 *   partition. If not, we stop tracking the respective partition.
 * 10-A timer thread is started by this object to track timeouts of partitions and drive the reading of their ideal
 *    state.
 *
 *  The following diagram illustrates the object interactions with main external APIs
 *
 *     (CONSUMING -> ONLINE state change)
 *             |
 *      markPartition(partitionId)      |<-recordPinotConsumptionDelay()-{LLRealtimeSegmentDataManager(Partition 0}}
 *            |                         |
 * ___________V_________________________V_
 * |           (Table X)                |<-recordPinotConsumptionDelay()-{LLRealtimeSegmentDataManager(Partition 1}}
 * | ConsumptionDelayTracker|           ...
 * |____________________________________|<-recordPinotConsumptionDelay()-{LLRealtimeSegmentDataManager (Partition n}}
 *              ^                      ^
 *              |                       \
 *   timeoutInactivePartitions()    stopTrackingPinotConsumptionDelay(partitionId)
 *    _________|__________                \
 *   | TimerTrackingTask |          (CONSUMING -> DROPPED state change)
 *   |___________________|
 *
 */

public class ConsumptionDelayTracker {

  // Sleep interval for timer thread that triggers read of ideal state
  private static final int TIMER_THREAD_TICK_INTERVAL_MS = 300000; // 5 minutes +/- precision in timeouts
  // Once a partition is marked for verification, we wait 10 minutes to pull its ideal state.
  private static final int PARTITION_TIMEOUT_MS = 600000;          // 10 minutes timeouts
  // Delay Timer thread for this time after starting timer
  private static final int INITIAL_TIMER_THREAD_DELAY_MS = 1000;

  /*
   * Class to keep a Pinot Consumption Delay measure and the time when the sample was taken (i.e. sample time)
   * We will use the sample time to increase consumption delay when a partition stops consuming: the time
   * difference between the sample time and current time will be added to the metric when read.
   */
  static private class DelayMeasure {
    public DelayMeasure(long t, long d) {
      _delayMilliseconds = d;
      _sampleTime = t;
    }
    public long _delayMilliseconds;
    public long _sampleTime;
  }

  /*
   * Class to handle timer thread that will track inactive partitions
   */
  private class TrackingTimerTask extends TimerTask {
    final private ConsumptionDelayTracker _tracker;

    public TrackingTimerTask(ConsumptionDelayTracker tracker) {
      _tracker = tracker;
    }

    @Override
    public void run() {
      // tick inactive partitions every interval of time to keep tracked partitions up to date
      _tracker.timeoutInactivePartitions();
    }
  }

  // HashMap used to store delay measures for all partitions active for the current table.
  // _partitionsToDelaySampleMap<Key=PartitionGroupId,Value=DelaySample>
  private ConcurrentHashMap<Integer, DelayMeasure> _partitionToDelaySampleMap = new ConcurrentHashMap<>();
  // We mark partitions that go from CONSUMING to ONLINE in _partitionsMarkedForVerification: if they do not
  // go back to CONSUMING in some period of time, we confirm whether they are still hosted in this server by reading
  // ideal state. This is done with the goal of minimizing reading ideal state for efficiency reasons.
  // _partitionsMarkedForVerification<Key=PartitionGroupId,Value=TimePartitionWasMarkedForVerificationMilliseconds>
  private ConcurrentHashMap<Integer, Long> _partitionsMarkedForVerification = new ConcurrentHashMap<>();
  // Mutable versions of timer constants so we can test with smaller delays
  final int _timerThreadTickIntervalMs;
  final int _initialTimeThreadDelayMs;
  // Timer task to check partitions that are inactive against ideal state.
  final private Timer _timer;

  final private ServerMetrics _serverMetrics;
  final private String _tableNameWithType;

  private boolean _enableAging;
  final private Logger _logger;

  final private RealtimeTableDataManager _realTimeTableDataManager;

  /*
   * Helper function to update the maximum when the current maximum is removed or updated.
   * If no samples left we set maximum to minimum so new samples can be recorded.
   */
  private DelayMeasure getMaximumDelay() {
    DelayMeasure newMax = null;
    for (int partitionGroupId : _partitionToDelaySampleMap.keySet()) {
      DelayMeasure currentMeasure = _partitionToDelaySampleMap.get(partitionGroupId);
      if ((newMax == null)
          ||
          (currentMeasure != null) && (currentMeasure._delayMilliseconds > newMax._delayMilliseconds)) {
        newMax = currentMeasure;
      }
    }
    return newMax;
  }

  private List<Integer> getPartitionsHostedByThisServerPerIdealState() {
    return _realTimeTableDataManager.getHostedPartitionsGroupIds();
  }
  /*
   * Helper function to be called when we should stop tracking a given partition. Removes the partition from
   * all our maps, it also updates the maximum if the tracked partition was the previous maximum.
   *
   * @param partitionGroupId partition ID which we should stop tracking.
   */
  private void removePartitionId(int partitionGroupId) {
    _partitionToDelaySampleMap.remove(partitionGroupId);
    // If we are removing a partition we should stop reading its ideal state.
    _partitionsMarkedForVerification.remove(partitionGroupId);
  }

  // Custom Constructor
  public ConsumptionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager realtimeTableDataManager, int initialTimeThreadDelayMs, int timerThreadTickIntervalMs)
      throws RuntimeException {
    _logger = LoggerFactory.getLogger(tableNameWithType + "-" + getClass().getSimpleName());
    _serverMetrics = serverMetrics;
    _tableNameWithType = tableNameWithType;
    _realTimeTableDataManager = realtimeTableDataManager;
    // Handle negative timer values
    if ((initialTimeThreadDelayMs < 0) || (timerThreadTickIntervalMs <= 0)) {
      throw new RuntimeException("Illegal timer arguments");
    }
    _enableAging = true;
    _initialTimeThreadDelayMs = initialTimeThreadDelayMs;
    _timerThreadTickIntervalMs = timerThreadTickIntervalMs;
    _timer = new Timer("ConsumptionDelayTimerThread" + tableNameWithType);
    _timer.schedule(new TrackingTimerTask(this), _initialTimeThreadDelayMs, _timerThreadTickIntervalMs);
    // Install callback metric
    _serverMetrics.addCallbackTableGaugeIfNeeded(_tableNameWithType, ServerGauge.MAX_PINOT_CONSUMPTION_DELAY_MS,
        () -> (long) getMaxPinotConsumptionDelay());
  }

  // Constructor that uses default timeout
  public ConsumptionDelayTracker(ServerMetrics serverMetrics, String tableNameWithType,
      RealtimeTableDataManager tableDataManager) {
    this(serverMetrics, tableNameWithType, tableDataManager, INITIAL_TIMER_THREAD_DELAY_MS,
        TIMER_THREAD_TICK_INTERVAL_MS);
  }

  /**
   * Use to set or rest the aging of reported values.
   * @param enableAging true if we want maximum to be aged as per sample time or false if we do not want to age
   *                   samples
   */
  @VisibleForTesting
  void setEnableAging(boolean enableAging) {
    _enableAging = enableAging;
  }

  /*
   * Called by LLRealTimeSegmentDataManagers to post delay updates to this tracker class.
   * If the new sample represents a new Maximum we update the current maximum.
   * If the new sample was for the partition that was maximum, but delay is not maximum anymore, we must select
   * a new maximum.
   *
   * @param delayInMilliseconds pinot consumption delay being recorded.
   * @param sampleTime sample time.
   * @param partitionGroupId partition ID for which this delay is being recorded.
   */
  public void storeConsumptionDelay(long delayInMilliseconds, long sampleTime, int partitionGroupId) {
    // Store new measure and wipe old one for this partition
    _partitionToDelaySampleMap.put(partitionGroupId, new DelayMeasure(sampleTime, delayInMilliseconds));
    // If we are consuming we do not need to track this partition for removal.
    _partitionsMarkedForVerification.remove(partitionGroupId);
  }

  /*
   * Handle partition removal event. This must be invoked when we stop serving a given partition for
   * this table in the current server.
   * This function will be invoked when we receive CONSUMING -> DROPPED / OFFLINE state transitions.
   *
   * @param partitionGroupId partition id that we should stop tracking.
   */
  public void stopTrackingPartitionConsumptionDelay(int partitionGroupId) {
    removePartitionId(partitionGroupId);
  }

  /*
   * This method is used for timing out inactive partitions, so we don't display their metrics on current server.
   * When the inactive time exceeds some threshold, we read from ideal state to confirm we still host the partition,
   * if not we remove the partition from being tracked locally.
   * This call is to be invoked by a timer thread that will periodically wake up and invoke this function.
   */
  public void timeoutInactivePartitions() {
    List<Integer> partitionsHostedByThisServer = null;
    try {
      partitionsHostedByThisServer = getPartitionsHostedByThisServerPerIdealState();
    } catch (Exception e) {
      _logger.error("Failed to get partitions hosted by this server");
      return;
    }
    HashSet<Integer> hostedPartitions = new HashSet(partitionsHostedByThisServer);
    for (int partitionGroupId : _partitionsMarkedForVerification.keySet()) {
      long markTime = _partitionsMarkedForVerification.get(partitionGroupId);
      long timeMarked = System.currentTimeMillis() - markTime;
      if ((timeMarked > PARTITION_TIMEOUT_MS) && (!hostedPartitions.contains(partitionGroupId))) {
        // Partition is not hosted in this server anymore, stop tracking it
        removePartitionId(partitionGroupId);
      }
    }
  }

  /*
   * This function is invoked when a partition goes from CONSUMING to ONLINE, so we can assert whether the
   * partition is still hosted by this server after some interval of time.
   *
   * @param partitionGroupId Partition id that we need confirmed via ideal state as still hosted by this server.
   */
  public void markPartitionForConfirmation(int partitionGroupId) {
    _partitionsMarkedForVerification.put(partitionGroupId, System.currentTimeMillis());
  }

  /*
   * This is the function to be invoked when reading the metric.
   * It reports the maximum Pinot Consumption delay for all partitions of this table being served
   * by current server; it adds the time elapsed since the sample was taken to the measure.
   * If no measures have been taken, then the reported value is zero.
   */
  public long getMaxPinotConsumptionDelay() {
    DelayMeasure currentMaxDelay = getMaximumDelay();
    if (currentMaxDelay == null) {
      return 0; // return 0 when not initialized
    }
    // Add age of measure to the reported value
    long measureAgeInMs = _enableAging ? (System.currentTimeMillis() - currentMaxDelay._sampleTime) : 0;
    // Correct to zero for any time shifts due to NTP or time reset.
    measureAgeInMs = Math.max(measureAgeInMs, 0);
    return currentMaxDelay._delayMilliseconds + measureAgeInMs;
  }

  /*
   * We use this method to clean up when a table is being removed. No updates are expected at this time
   * as all LLRealtimeSegmentManagers should be down now.
   */
  public void shutdown() {
    // Now that segments can't report metric, destroy metric for this table
    _timer.cancel();
    _serverMetrics.removeTableGauge(_tableNameWithType, ServerGauge.MAX_PINOT_CONSUMPTION_DELAY_MS);
  }
}