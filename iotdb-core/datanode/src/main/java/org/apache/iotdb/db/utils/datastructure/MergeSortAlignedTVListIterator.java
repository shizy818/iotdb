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
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.db.utils.datastructure.TVList.ERR_DATATYPE_NOT_CONSISTENT;

public class MergeSortAlignedTVListIterator implements IPointReader {
  private final AlignedTVList.AlignedTVListIterator[] alignedTvListIterators;
  private boolean probeNext = false;
  private boolean validRow = false;
  private long currentTime;
  private Object[] currentValues;
  private final List<Integer> columnIndexList;
  private final List<TSDataType> dataTypeList;

  private final int[] alignedTvListOffsets;

  public MergeSortAlignedTVListIterator(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> dataTypeList,
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
    this.columnIndexList = columnIndexList;
    this.dataTypeList = dataTypeList;
  }

  private TimeValuePair buildTimeValuePair(long time, Object[] values) {
    TsPrimitiveType[] vector = new TsPrimitiveType[values.length];
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        continue;
      }

      TSDataType tsDataType =
          columnIndexList == null ? dataTypeList.get(i) : dataTypeList.get(columnIndexList.get(i));
      switch (tsDataType) {
        case BOOLEAN:
          vector[i] = TsPrimitiveType.getByType(TSDataType.BOOLEAN, values[i]);
          break;
        case INT32:
        case DATE:
          vector[i] = TsPrimitiveType.getByType(TSDataType.INT32, values[i]);
          break;
        case INT64:
        case TIMESTAMP:
          vector[i] = TsPrimitiveType.getByType(TSDataType.INT64, values[i]);
          break;
        case FLOAT:
          vector[i] = TsPrimitiveType.getByType(TSDataType.FLOAT, values[i]);
          break;
        case DOUBLE:
          vector[i] = TsPrimitiveType.getByType(TSDataType.DOUBLE, values[i]);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          vector[i] = TsPrimitiveType.getByType(TSDataType.TEXT, values[i]);
          break;
        default:
          throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
      }
    }
    return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.VECTOR, vector));
  }

  private void prepareNextRow() {
    validRow = false;
    long time = Long.MAX_VALUE;
    for (AlignedTVList.AlignedTVListIterator iterator : alignedTvListIterators) {
      if (iterator.hasNext() && iterator.currentTime() <= time) {
        Object[] values = iterator.current();
        if (!validRow || iterator.currentTime() < time) {
          currentValues = values;
          currentTime = iterator.currentTime();
          validRow = true;
        } else {
          for (int columnIndex = 0; columnIndex < values.length; columnIndex++) {
            // update currentTvPair if the column is not null
            if (values[columnIndex] != null) {
              currentValues[columnIndex] = values[columnIndex];
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
    return validRow;
  }

  public long currentTime() {
    return currentTime;
  }

  public Object[] currentValues() {
    return currentValues;
  }

  public void step() {
    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasCurrent() && iterator.currentTime() == currentTime) {
        alignedTvListIterators[i].step();
        alignedTvListOffsets[i] = alignedTvListIterators[i].getIndex();
      }
    }
    probeNext = false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }

    for (int i = 0; i < alignedTvListIterators.length; i++) {
      AlignedTVList.AlignedTVListIterator iterator = alignedTvListIterators[i];
      if (iterator.hasCurrent() && iterator.currentTime() == currentTime) {
        alignedTvListIterators[i].step();
        alignedTvListOffsets[i] = alignedTvListIterators[i].getIndex();
      }
    }
    probeNext = false;
    validRow = false;
    return buildTimeValuePair(currentTime, currentValues);
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    return buildTimeValuePair(currentTime, currentValues);
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
    validRow = false;
  }
}
