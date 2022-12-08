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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;


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
 * | MaximumPinotConsumptionDelayTracker|           ...
 * |____________________________________|<-recordPinotConsumptionDelay()-{LLRealtimeSegmentDataManager (Partition n}}
 *              ^                      ^
 *              |                       \
 *   tickInactivePartitions()    stopTrackingPinotConsumptionDelay(partitionId)
 *    _________|__________                \
 *   | TimerTrackingTask |          (CONSUMING -> DROPPED state change)
 *   |___________________|
 *
 */

public class MaximumPinotConsumptionDelayTracker {
  /*
   * Class to handle timer thread that will track inactive partitions
   */
  private class TrackingTimerTask extends TimerTask {
    private String _name;
    private MaximumPinotConsumptionDelayTracker _tracker;

    public TrackingTimerTask(String name, MaximumPinotConsumptionDelayTracker tracker) {
      _name = name;
      _tracker = tracker;
    }

    @Override
    public void run() {
      // tick inactive partitions every interval of time to keep tracked partitions up to date
      _tracker.tickInactivePartitions();
    }
  }

  /*
   * Class to keep a Pinot Consumption Delay measure and the time when the sample was taken (i.e. sample time)
   * We will use the sample time to increase consumption delay when a partition stops consuming: the time
   * difference between the sample time and current time will be added to the metric when read.
   */
  private class DelayMeasure {
    public DelayMeasure(long t, long d) {
      _delayMilliseconds = d;
      _sampleTime = t;
    }
    public long _delayMilliseconds;
    public long _sampleTime;
  }

  // HashMap used to store delay measures for all partitions active for the current table.
  private ConcurrentHashMap<Long, DelayMeasure> _partitionToDelaySampleMap = new ConcurrentHashMap<>();
  // We mark partitions that go from CONSUMING to ONLINE in _partitionsMarkedForConfirmation: if they do not
  // go back to CONSUMING in some timeout, we confirm they are still hosted in this server by reading
  // ideal state. This is done with the goal of minimizing reading ideal state for efficiency reasons.
  // We do tick up counts associated with inactive partitions so that we can verify they are still
  // hosted by the current server after a preconfigured number of ticks.
  private ConcurrentHashMap<Long, Integer> _partitionsMarkedForConfirmation = new ConcurrentHashMap<>();
  // Check ideal state only after _MAX_NUMBER_OF_TIME_TICKS_BEFORE_VERIFICATION * _TIMER_THREAD_TICK_INTERVAL_MS
  private static final int MAX_NUMBER_OF_TIME_TICKS_BEFORE_VERIFICATION = 5;
  // Sleep interval for timer thread that triggers read of ideal state
  private static final int TIMER_THREAD_TICK_INTERVAL_MS = 60000;
  // Delay Timer thread for this time after starting timer
  private static final int INITIAL_TIMER_THREAD_DELAY_MS = 1000;
  // Mutable versions of the above constants so we can test with smaller delays
  int _maxNumberOfTicksBeforeVerification;
  int _timerThreadTickIntervalMs;
  int _initialTimeThreadDelayMs;

  // The following two fields are used to track the current maximum delay and the reporting partition.
  // private long _maxPinotConsumptionDelay;
  // private long _maxPartitionId;

  // Timer task to check partitions that are inactive against ideal state.
  private TrackingTimerTask _timerTask;
  private Timer _timer;

  private ServerMetrics _serverMetrics;
  private String _tableNameWithType;

  private boolean _enableAging;

  RealtimeTableDataManager _realTimeTableDataManager;

  /*
   * Helper function to update the maximum when the current maximum is removed or updated.
   * If no samples left we set maximum to minimum so new samples can be recorded.
   */
  private DelayMeasure getMaximumDelay() {
    DelayMeasure newMax = null;
    for (long pid : _partitionToDelaySampleMap.keySet()) {
      DelayMeasure currentMeasure = _partitionToDelaySampleMap.get(pid);
      if ((newMax == null) || (currentMeasure._delayMilliseconds > newMax._delayMilliseconds)) {
        newMax = currentMeasure;
      }
    }
    return newMax;
  }

  /*
   * This function will read ideal state for the given partition and return true if ideal state reflects the
   * partition must be hosted by the current server, false otherwise.
   *
   * @param partitionId Partition ID that we want to verify is still hosted by this server.
   */
  private boolean isPartitionHostedByThisServerPerIdealState(long partitionId) {
    return _realTimeTableDataManager.isPartitionHostedInThisServer(partitionId);
  }

  /*
   * Helper function to be called when we should stop tracking a given partition. Removes the partition from
   * all our maps, it also updates the maximum if the tracked partition was the previous maximum.
   *
   * @param partitionId partition ID which we should stop tracking.
   */
  private void removePartitionId(long partitionId) {
    _partitionToDelaySampleMap.remove(partitionId);
    // If we are removing a partition we should stop reading its ideal state.
    _partitionsMarkedForConfirmation.remove(partitionId);
   }

  // Custom Constructor
  public MaximumPinotConsumptionDelayTracker(ServerMetrics sm, String tableNameWithType, RealtimeTableDataManager tdm,
      int maxNumberOfTicksBeforeVerification, int initialTimeThreadDelayMs, int timerThreadTickIntervalMs)
      throws RuntimeException {
    _serverMetrics = sm;
    _tableNameWithType = tableNameWithType;
    _realTimeTableDataManager = tdm;
    // TBD handle negative timer values
    if ((maxNumberOfTicksBeforeVerification < 0) || (initialTimeThreadDelayMs < 0)
        || (timerThreadTickIntervalMs <= 0)) {
      throw new RuntimeException("Illegal timer arguments");
    }
    _enableAging = true;
    _maxNumberOfTicksBeforeVerification = maxNumberOfTicksBeforeVerification;
    _initialTimeThreadDelayMs = initialTimeThreadDelayMs;
    _timerThreadTickIntervalMs = timerThreadTickIntervalMs;
    _timerTask = new TrackingTimerTask("TimerTask" + tableNameWithType, this);
    _timer = new Timer();
    _timer.schedule(_timerTask, _initialTimeThreadDelayMs, _timerThreadTickIntervalMs);
    // Install callback metric
    _serverMetrics.addCallbackTableGaugeIfNeeded(_tableNameWithType, ServerGauge.MAX_PINOT_CONSUMPTION_DELAY_MS,
        () -> (long) getMaxPinotConsumptionDelay());
  }

  // Constructor that uses default timeout
  public MaximumPinotConsumptionDelayTracker(ServerMetrics sm, String tableNameWithType,
      RealtimeTableDataManager tdm) {
    this(sm, tableNameWithType, tdm, MAX_NUMBER_OF_TIME_TICKS_BEFORE_VERIFICATION,
        INITIAL_TIMER_THREAD_DELAY_MS, TIMER_THREAD_TICK_INTERVAL_MS);
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
   * @param currentTime sample time.
   * @param partitionId partition ID for which this delay is being recorded.
   */
  public void recordPinotConsumptionDelay(long delayInMilliseconds, long currentTime, long partitionId) {
    // Store new measure and wipe old one for this partition
    _partitionToDelaySampleMap.put(partitionId, new DelayMeasure(currentTime, delayInMilliseconds));
    // If we are consuming we do not need to track this partition for removal.
    _partitionsMarkedForConfirmation.remove(partitionId);
  }

  /*
   * Handle partition removal event. This must be invoked when we stop serving a given partition for
   * this table in the current server.
   * This function will be invoked when we receive CONSUMING -> DROPPED / OFFLINE state transitions.
   *
   * @param partitionId partition id that we should stop tracking.
   */
  public void stopTrackingPinotConsumptionDelayForPartition(long partitionId) {
    removePartitionId(partitionId);
  }

  /*
   * This method is used for ticking up the time a partition has been out of CONSUMING state.
   * When that time exceeds some threshold, we read from ideal state to confirm we still host the partition, if
   * not we remove the partition from being tracked locally.
   * This call is to be invoked by a timer thread that will periodically wake up and invoke this function.
   */
  public void tickInactivePartitions() {
    for (long partitionId : _partitionsMarkedForConfirmation.keySet()) {
      int currentTicks = _partitionsMarkedForConfirmation.get(partitionId);
      if (++currentTicks >= _maxNumberOfTicksBeforeVerification) {
        if (!isPartitionHostedByThisServerPerIdealState(partitionId)) {
          // If this partition is not hosted by this host as per ideal state we stop tracking
          removePartitionId(partitionId);
          return;
        } else {
          // Partition is hosted by us as per ideal state, hence do not track for removal
          // Reset timeout and try later if it continues to not consume.
          _partitionsMarkedForConfirmation.put(partitionId, 0);
        }
      } else {
        // If not enough time in inactive state, we just add to the timeout count
        _partitionsMarkedForConfirmation.put(partitionId, currentTicks);
      }
    }
  }

  /*
   * This function is invoked when a partition goes from CONSUMING to ONLINE, so we can assert whether the
   * partition is still hosted by this server after some interval of time.
   *
   * @param partitionId Partition id that we need confirmed via ideal state as still hosted by this server.
   */
  public void markPartitionForConfirmation(long partitionId) {
    _partitionsMarkedForConfirmation.put(partitionId, 0);
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
    measureAgeInMs = measureAgeInMs >= 0 ? measureAgeInMs : 0;
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
