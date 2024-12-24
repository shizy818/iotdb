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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

public class MergeSortAlignedTVListIterator implements IPointReader {
  private final AlignedTVList.AlignedTVListIterator[] alignedTvListIterators;
  private final List<TSDataType> tsDataTypes;

  private boolean probeNext = false;
  private boolean hasNext = false;

  private final int[] alignedTvListOffsets;

  private final int[][] columnAccessInfo;
  private long time;
  private final BitMap bitMap;

  public MergeSortAlignedTVListIterator(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    this.alignedTvListIterators = new AlignedTVList.AlignedTVListIterator[alignedTvLists.size()];
    for (int i = 0; i < alignedTvLists.size(); i++) {
      alignedTvListIterators[i] =
          alignedTvLists
              .get(i)
              .iterator(
                  tsDataTypes, columnIndexList, ignoreAllNullRows, floatPrecision, encodingList);
    }
    this.alignedTvListOffsets = new int[alignedTvLists.size()];
    this.tsDataTypes = tsDataTypes;
    this.columnAccessInfo = new int[tsDataTypes.size()][];
    this.bitMap = new BitMap(tsDataTypes.size());
  }

  private void prepareNextRow() {
    time = Long.MAX_VALUE;
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasNext() && iterator.currentTime() <= time) {
        if (i == 0 || iterator.currentTime() < time) {
          for (int columnIndex = 0; columnIndex < tsDataTypes.size(); columnIndex++) {
            int rowIndex = iterator.getValidRowIndex(columnIndex);
            columnAccessInfo[columnIndex] = new int[] {i, rowIndex};
            if (rowIndex < 0) {
              bitMap.mark(columnIndex);
            }
          }
          time = iterator.currentTime();
        } else {
          for (int columnIndex = 0; columnIndex < tsDataTypes.size(); columnIndex++) {
            int rowIndex = iterator.getValidRowIndex(columnIndex);
            // update if the column is not null
            if (rowIndex >= 0) {
              columnAccessInfo[columnIndex][0] = i;
              columnAccessInfo[columnIndex][1] = rowIndex;
              bitMap.unmark(columnIndex);
            }
          }
        }
        hasNext = true;
      }
    }
    probeNext = true;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNextRow();
    }
    return hasNext;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TimeValuePair tvPair = buildTimeValuePair();
    step();
    return tvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    return buildTimeValuePair();
  }

  private TimeValuePair buildTimeValuePair() {
    TsPrimitiveType[] vector = new TsPrimitiveType[tsDataTypes.size()];
    for (int columnIndex = 0; columnIndex < vector.length; columnIndex++) {
      int[] accessInfo = columnAccessInfo[columnIndex];
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[accessInfo[0]];
      vector[columnIndex] = iterator.getPrimitiveObject(accessInfo[1], columnIndex);
    }
    return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.VECTOR, vector));
  }

  public void step() {
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasCurrent() && iterator.currentTime() == time) {
        alignedTvListIterators[i].step();
        alignedTvListOffsets[i] = alignedTvListIterators[i].getIndex();
      }
    }
    probeNext = false;
    hasNext = false;
    bitMap.reset();
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  @Override
  public void close() throws IOException {}

  public int[] getAlignedTVListOffsets() {
    return alignedTvListOffsets;
  }

  public void setAlignedTVListOffsets(int[] alignedTvListOffsets) {
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      alignedTvListIterators[i].setIndex(alignedTvListOffsets[i]);
      this.alignedTvListOffsets[i] = alignedTvListOffsets[i];
    }
    probeNext = false;
    hasNext = false;
    bitMap.reset();
  }

  public int[][] getColumnAccessInfo() {
    return columnAccessInfo;
  }

  public long getTime() {
    return time;
  }

  public TsPrimitiveType getPrimitiveObject(int[] accessInfo, int columnIndex) {
    if (columnIndex >= columnAccessInfo.length) {
      return null;
    }
    AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[accessInfo[0]];
    return iterator.getPrimitiveObject(accessInfo[1], columnIndex);
  }

  public BitMap getBitmap() {
    return bitMap;
  }

  public int[] getAlignedTvListIndex() {
    int[] ret = new int[alignedTvListIterators.length];
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      ret[i] = alignedTvListIterators[i].getIndex();
    }
    return ret;
  }

  public int[] getLimits() {
    int[] ret = new int[alignedTvListIterators.length];
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      ret[i] = alignedTvListIterators[i].getRows();
    }
    return ret;
  }

  public void setTVListLimit(List<Integer> limit) {
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      alignedTvListIterators[i].setRows(limit.get(i));
    }
  }
}
