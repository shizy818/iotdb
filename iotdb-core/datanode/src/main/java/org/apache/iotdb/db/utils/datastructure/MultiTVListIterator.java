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

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class MultiTVListIterator implements IPointReader {
  protected TSDataType tsDataType;
  protected List<TVList.TVListIterator> tvListIterators;
  protected List<TsBlock> tsBlocks;
  protected Integer floatPrecision;
  protected TSEncoding encoding;
  protected List<TimeRange> deletionList;

  protected boolean probeNext = false;
  protected boolean hasNext = false;
  protected int iteratorIndex = 0;
  protected int rowIndex = 0;

  protected final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  protected MultiTVListIterator() {}

  protected MultiTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    this.tsDataType = tsDataType;
    this.tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(deletionList));
    }
    this.deletionList = deletionList;
    this.floatPrecision = floatPrecision;
    this.encoding = encoding;
    this.tsBlocks = new ArrayList<>();
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNext();
    }
    return hasNext;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    TimeValuePair currentTvPair =
        iterator
            .getTVList()
            .getTimeValuePair(rowIndex, iterator.currentTime(), floatPrecision, encoding);
    next();
    return currentTvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    return iterator
        .getTVList()
        .getTimeValuePair(rowIndex, iterator.currentTime(), floatPrecision, encoding);
  }

  public boolean hasNextBatch(int tsBlockIndex) {
    if (tsBlocks == null) {
      return false;
    }
    return tsBlockIndex < tsBlocks.size();
  }

  public boolean hasNextBatch() {
    return hasNextTimeValuePair();
  }

  public TsBlock nextBatch(int tsBlockIndex) {
    if (tsBlockIndex < 0 || tsBlockIndex >= tsBlocks.size()) {
      return null;
    }
    TsBlock tsBlock = tsBlocks.get(tsBlockIndex);
    tsBlocks.set(tsBlockIndex, null);
    return tsBlock;
  }

  public TsBlock nextBatch() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(tsDataType));
    int pointsInBatch = 0;
    while (hasNextTimeValuePair() && pointsInBatch < MAX_NUMBER_OF_POINTS_IN_PAGE) {
      TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
      builder.getTimeColumnBuilder().writeLong(iterator.currentTime());
      switch (tsDataType) {
        case BOOLEAN:
          builder.getColumnBuilder(0).writeBoolean(iterator.getTVList().getBoolean(rowIndex));
          break;
        case INT32:
        case DATE:
          builder.getColumnBuilder(0).writeInt(iterator.getTVList().getInt(rowIndex));
          break;
        case INT64:
        case TIMESTAMP:
          builder.getColumnBuilder(0).writeLong(iterator.getTVList().getLong(rowIndex));
          break;
        case FLOAT:
          float fValue = iterator.getTVList().getFloat(rowIndex);
          if (!Float.isNaN(fValue)
              && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
            fValue = MathUtils.roundWithGivenPrecision(fValue, floatPrecision);
          }
          builder.getColumnBuilder(0).writeFloat(fValue);
          break;
        case DOUBLE:
          double dValue = iterator.getTVList().getDouble(rowIndex);
          if (!Double.isNaN(dValue)
              && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
            dValue = MathUtils.roundWithGivenPrecision(dValue, floatPrecision);
          }
          builder.getColumnBuilder(0).writeDouble(dValue);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          builder.getColumnBuilder(0).writeBinary(iterator.getTVList().getBinary(rowIndex));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", tsDataType));
      }
      next();

      builder.declarePosition();
      pointsInBatch++;
    }
    TsBlock tsBlock = builder.build();
    tsBlocks.add(tsBlock);
    return tsBlock;
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  @Override
  public void close() throws IOException {}

  public abstract MultiTVListIterator clone();

  protected abstract void prepareNext();

  protected abstract void next();
}
