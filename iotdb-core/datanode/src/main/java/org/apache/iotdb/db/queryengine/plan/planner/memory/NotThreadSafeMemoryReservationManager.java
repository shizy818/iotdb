/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner.memory;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class NotThreadSafeMemoryReservationManager implements MemoryReservationManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(NotThreadSafeMemoryReservationManager.class);

  // To avoid reserving memory too frequently, we choose to do it in batches. This is the lower
  // bound for each batch.
  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;

  private final LocalExecutionPlanner LOCAL_EXECUTION_PLANNER = LocalExecutionPlanner.getInstance();

  private final QueryId queryId;

  private final String contextHolder;

  private long reservedBytesInTotal = 0;

  private long bytesToBeReserved = 0;

  private long bytesToBeReleased = 0;

  public NotThreadSafeMemoryReservationManager(final QueryId queryId, final String contextHolder) {
    this.queryId = queryId;
    this.contextHolder = contextHolder;
  }

  @Override
  public void reserveMemoryCumulatively(final long size, String caller) {
    bytesToBeReserved += size;
    if (bytesToBeReserved >= MEMORY_BATCH_THRESHOLD) {
      LOCAL_EXECUTION_PLANNER.reserveFromFreeMemoryForOperators(
          bytesToBeReserved, reservedBytesInTotal, queryId.getId(), contextHolder, caller);
      reservedBytesInTotal += bytesToBeReserved;
      bytesToBeReserved = 0;
    }
    LOGGER.info(
        "reserveMemoryCumulatively by {}, size {}, bytesToBeReserved {}, bytesToBeReleased {}, reservedBytesInTotal {}, MemoryReservationManager {}, AtomicLongMemoryBlock {}",
        caller,
        size,
        bytesToBeReserved,
        bytesToBeReleased,
        reservedBytesInTotal,
        this,
        LOCAL_EXECUTION_PLANNER.getOperatorsMemoryBlock());
  }

  @Override
  public void reserveMemoryImmediately(String caller) {
    if (bytesToBeReserved != 0) {
      LOCAL_EXECUTION_PLANNER.reserveFromFreeMemoryForOperators(
          bytesToBeReserved, reservedBytesInTotal, queryId.getId(), contextHolder, caller);
      reservedBytesInTotal += bytesToBeReserved;
      bytesToBeReserved = 0;
      LOGGER.info(
          "reserveMemoryImmediately by {}, bytesToBeReserved {}, bytesToBeReleased {}, reservedBytesInTotal {}, MemoryReservationManager {}, AtomicLongMemoryBlock {}",
          caller,
          bytesToBeReserved,
          bytesToBeReleased,
          reservedBytesInTotal,
          this,
          LOCAL_EXECUTION_PLANNER.getOperatorsMemoryBlock());
    }
  }

  @Override
  public void releaseMemoryCumulatively(final long size, String caller) {
    bytesToBeReleased += size;
    if (bytesToBeReleased >= MEMORY_BATCH_THRESHOLD) {
      long bytesToRelease;
      if (bytesToBeReleased <= bytesToBeReserved) {
        bytesToBeReserved -= bytesToBeReleased;
      } else {
        bytesToRelease = bytesToBeReleased - bytesToBeReserved;
        bytesToBeReserved = 0;
        LOCAL_EXECUTION_PLANNER.releaseToFreeMemoryForOperators(bytesToRelease, caller);
        reservedBytesInTotal -= bytesToRelease;
      }
      bytesToBeReleased = 0;
    }
    LOGGER.info(
        "releaseMemoryCumulatively by {}, size {}, bytesToBeReserved {}, bytesToBeReleased {}, reservedBytesInTotal {}, MemoryReservationManager {}, AtomicLongMemoryBlock {}",
        caller,
        size,
        bytesToBeReserved,
        bytesToBeReleased,
        reservedBytesInTotal,
        this,
        LOCAL_EXECUTION_PLANNER.getOperatorsMemoryBlock());
  }

  @Override
  public void releaseAllReservedMemory(String caller) {
    if (reservedBytesInTotal != 0) {
      LOCAL_EXECUTION_PLANNER.releaseToFreeMemoryForOperators(reservedBytesInTotal, caller);
      reservedBytesInTotal = 0;
      bytesToBeReserved = 0;
      bytesToBeReleased = 0;
    }
    LOGGER.info(
        "releaseAllReservedMemory by {}, bytesToBeReserved {}, bytesToBeReleased {}, reservedBytesInTotal {}, MemoryReservationManager {}, AtomicLongMemoryBlock {}",
        caller,
        bytesToBeReserved,
        bytesToBeReleased,
        reservedBytesInTotal,
        this,
        LOCAL_EXECUTION_PLANNER.getOperatorsMemoryBlock());
  }
}
