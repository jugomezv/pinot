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

import java.util.Map;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.stream.ConsumerPartitionState;
import org.apache.pinot.spi.stream.PartitionLagState;
import org.apache.pinot.spi.utils.CommonConstants.ConsumerState;


public abstract class RealtimeSegmentDataManager extends SegmentDataManager {

  @Override
  public abstract MutableSegment getSegment();

  protected static PinotDataBufferMemoryManager getMemoryManager(String consumerDir, String segmentName,
      boolean offHeap, boolean directOffHeap, ServerMetrics serverMetrics) {
    if (offHeap && !directOffHeap) {
      return new MmapMemoryManager(consumerDir, segmentName, serverMetrics);
    } else {
      // For on-heap allocation, we still need a memory manager for forward index.
      // Dictionary will be allocated on heap.
      return new DirectMemoryManager(segmentName, serverMetrics);
    }
  }

  /**
   * Get the current offsets for all partitions of this consumer
   */
  public abstract Map<String, String> getPartitionToCurrentOffset();

  /**
   * Get the state of the consumer
   */
  public abstract ConsumerState getConsumerState();

  /**
   * @return Timestamp at which the last record was indexed
   */
  public abstract long getLastConsumedTimestamp();

  /**
   * @return Per-partition consumer's status, which typically includes last consumed message timestamp,
   * latest available upstream offset etc
   */
  public abstract Map<String, ConsumerPartitionState> getConsumerPartitionState();

  public abstract Map<String, PartitionLagState> getPartitionToLagState(
      Map<String, ConsumerPartitionState> consumerPartitionStateMap);

  /**
   * The RT segment data manager can handle status change from external components like the ConsumptionStatusChecker
   * etc. Currently, it acts as a way to signal the RT Segment data manager that the current partition has caught up.
   *
   * @param caughtUpWithUpstream Boolean indicating if the partition has caught up with upstream source or not based on
   *                            the strategy used in the {@literal IngestionBasedConsumptionStatusChecker}
   */
  public abstract void notifyConsumptionCaughtUp(boolean caughtUpWithUpstream);
}
