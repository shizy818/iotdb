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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.BitMap;

import java.util.ArrayList;
import java.util.List;

public class MemAlignedChunkHandleImpl extends MemChunkHandleImpl {
  private final BitMap bitMapOfValue;

  public MemAlignedChunkHandleImpl(
      IDeviceID deviceID, String measurement, long[] dataOfTimestamp, BitMap bitMapOfValue) {
    super(deviceID, measurement, dataOfTimestamp);
    this.bitMapOfValue = bitMapOfValue;
  }

  @Override
  public long[] getPageStatisticsTime() {
    if (bitMapOfValue == null) {
      return new long[] {dataOfTimestamp[pageStart], dataOfTimestamp[pageEnd - 1]};
    }
    long startTime = -1, endTime = -1;
    for (int i = pageStart; i < pageEnd; i++) {
      if (!bitMapOfValue.isMarked(i)) {
        startTime = dataOfTimestamp[i];
        break;
      }
    }

    for (int i = pageEnd - 1; i >= pageStart; i--) {
      if (!bitMapOfValue.isMarked(i)) {
        endTime = dataOfTimestamp[i];
        break;
      }
    }
    return new long[] {startTime, endTime};
  }

  @Override
  public long[] getDataTime() {
    List<Long> timeList = new ArrayList<>();
    for (int i = pageStart; i < pageEnd; i++) {
      if (bitMapOfValue != null && bitMapOfValue.isMarked(i)) {
        continue;
      }
      timeList.add(dataOfTimestamp[i]);
      //      if (!ModificationUtils.isPointDeleted(dataOfTimestamp[i], deletionList,
      // deletionCursor)
      //          && (i == dataOfTimestamp.length - 1 || dataOfTimestamp[i] != dataOfTimestamp[i +
      // 1])) {
      //        timeList.add(dataOfTimestamp[i]);
      //      }
    }
    return timeList.stream().mapToLong(Long::longValue).toArray();
  }
}
