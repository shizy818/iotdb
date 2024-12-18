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

import org.apache.iotdb.db.utils.MathUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

public class MergeSortAlignedTVListIterator implements IPointReader {
  private final AlignedTVList.AlignedTVListIterator[] alignedTvListIterators;
  private boolean probeNext = false;
  private TimeValuePair currentTvPair;

  private final int[] alignedTvListOffsets;

  private Integer floatPrecision;
  private List<TSEncoding> encodingList;

  public MergeSortAlignedTVListIterator(
      List<AlignedTVList> alignedTvLists,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows) {
    this.alignedTvListIterators = new AlignedTVList.AlignedTVListIterator[alignedTvLists.size()];
    for (int i = 0; i < alignedTvLists.size(); i++) {
      alignedTvListIterators[i] =
          alignedTvLists
              .get(i)
              .iterator(
                  columnIndexList,
                  ignoreAllNullRows,
                  floatPrecision,
                  encodingList,
                  timeColumnDeletion,
                  valueColumnsDeletionList);
    }
    this.alignedTvListOffsets = new int[alignedTvLists.size()];
    this.floatPrecision = floatPrecision;
    this.encodingList = encodingList;
  }

  private void prepareNextRow() {
    currentTvPair = null;
    long time = Long.MAX_VALUE;
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasNext() && iterator.currentTime() <= time) {
        TimeValuePair tvPair = iterator.current();
        // check valueColumnsDeletionList
        if (currentTvPair == null || iterator.currentTime() < time) {
          currentTvPair = tvPair;
        } else {
          TsPrimitiveType[] primitiveValues = tvPair.getValue().getVector();
          for (int columnIndex = 0; columnIndex < primitiveValues.length; columnIndex++) {
            // update currentTvPair if the column is not null
            if (primitiveValues[columnIndex] != null) {
              currentTvPair.getValue().getVector()[columnIndex] = primitiveValues[columnIndex];
            }
          }
        }
        time = iterator.currentTime();
      }
    }
    probeNext = true;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNextRow();
    }
    return currentTvPair != null;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }

    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasCurrent() && iterator.currentTime() == currentTvPair.getTimestamp()) {
        alignedTvListIterators[i].step();
        alignedTvListOffsets[i] = alignedTvListIterators[i].getIndex();
      }
    }
    probeNext = false;
    return getCurrentTvPair();
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    return getCurrentTvPair();
  }

  public TimeValuePair getCurrentTvPair() {
    if (encodingList != null && floatPrecision != -1) {
      TsPrimitiveType[] primitiveValues = currentTvPair.getValue().getVector();
      for (int i = 0; i < primitiveValues.length; i++) {
        if (primitiveValues[i] == null) {
          continue;
        }
        if (primitiveValues[i].getDataType() == TSDataType.FLOAT) {
          float valueF = primitiveValues[i].getFloat();
          if (!Float.isNaN(valueF)
              && (encodingList.get(i) == TSEncoding.RLE
                  || encodingList.get(i) == TSEncoding.TS_2DIFF)) {
            primitiveValues[i].setFloat(MathUtils.roundWithGivenPrecision(valueF, floatPrecision));
          }
        }
        if (primitiveValues[i].getDataType() == TSDataType.DOUBLE) {
          double valueD = primitiveValues[i].getDouble();
          if (!Double.isNaN(valueD)
              && (encodingList.get(i) == TSEncoding.RLE
                  || encodingList.get(i) == TSEncoding.TS_2DIFF)) {
            primitiveValues[i].setDouble(MathUtils.roundWithGivenPrecision(valueD, floatPrecision));
          }
        }
      }
    }
    return currentTvPair;
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
  }
}
