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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;

public abstract class MultiTVListIterator implements MemPointIterator {
  protected TSDataType tsDataType;
  protected List<TVList.TVListIterator> tvListIterators;
  protected List<TsBlock> tsBlocks;
  protected int floatPrecision;
  protected TSEncoding encoding;

  protected boolean probeNext = false;
  protected boolean hasNext = false;
  protected int iteratorIndex = 0;
  protected int rowIndex = 0;

  protected final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private final long TARGET_CHUNK_SIZE = CONFIG.getTargetChunkSize();
  private final long MAX_NUMBER_OF_POINTS_IN_CHUNK = CONFIG.getTargetChunkPointNum();

  protected MultiTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    this.tsDataType = tsDataType;
    this.tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(deletionList, null, null));
    }
    this.floatPrecision = floatPrecision != null ? floatPrecision : 0;
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

  @Override
  public boolean hasNextBatch() {
    return hasNextTimeValuePair();
  }

  @Override
  public TsBlock nextBatch() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(tsDataType));
    while (hasNextTimeValuePair() && builder.getPositionCount() < MAX_NUMBER_OF_POINTS_IN_PAGE) {
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
          TVList floatTvList = iterator.getTVList();
          builder
              .getColumnBuilder(0)
              .writeFloat(
                  floatTvList.roundValueWithGivenPrecision(
                      floatTvList.getFloat(rowIndex), floatPrecision, encoding));
          break;
        case DOUBLE:
          TVList doubleTvList = iterator.getTVList();
          builder
              .getColumnBuilder(0)
              .writeDouble(
                  doubleTvList.roundValueWithGivenPrecision(
                      doubleTvList.getDouble(rowIndex), floatPrecision, encoding));
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
    }
    TsBlock tsBlock = builder.build();
    tsBlocks.add(tsBlock);
    return tsBlock;
  }

  @Override
  public void batchEncode(IChunkWriter chunkWriter, BatchEncodeInfo encodeInfo, long[] times) {
    ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;
    while (hasNextTimeValuePair()) {
      // remember current iterator and row index
      TVList.TVListIterator currIterator = tvListIterators.get(iteratorIndex);
      long time = currIterator.currentTime();
      int currRowIndex = rowIndex;

      // check if it is last point
      next();
      if (!hasNextTimeValuePair()) {
        chunkWriterImpl.setLastPoint(true);
      }

      switch (tsDataType) {
        case BOOLEAN:
          chunkWriterImpl.write(time, currIterator.getTVList().getBoolean(currRowIndex));
          encodeInfo.dataSizeInChunk += 8L + 1L;
          break;
        case INT32:
        case DATE:
          chunkWriterImpl.write(time, currIterator.getTVList().getInt(currRowIndex));
          encodeInfo.dataSizeInChunk += 8L + 4L;
          break;
        case INT64:
        case TIMESTAMP:
          chunkWriterImpl.write(time, currIterator.getTVList().getLong(currRowIndex));
          encodeInfo.dataSizeInChunk += 8L + 8L;
          break;
        case FLOAT:
          TVList floatTvList = currIterator.getTVList();
          chunkWriterImpl.write(
              time,
              floatTvList.roundValueWithGivenPrecision(
                  floatTvList.getFloat(currRowIndex), floatPrecision, encoding));
          encodeInfo.dataSizeInChunk += 8L + 4L;
          break;
        case DOUBLE:
          TVList doubleTvList = currIterator.getTVList();
          chunkWriterImpl.write(
              time,
              doubleTvList.roundValueWithGivenPrecision(
                  doubleTvList.getDouble(currRowIndex), floatPrecision, encoding));
          encodeInfo.dataSizeInChunk += 8L + 8L;
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary value = currIterator.getTVList().getBinary(currRowIndex);
          chunkWriterImpl.write(time, value);
          encodeInfo.dataSizeInChunk += 8L + getBinarySize(value);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", tsDataType));
      }
      encodeInfo.pointNumInChunk++;

      if (encodeInfo.pointNumInChunk >= MAX_NUMBER_OF_POINTS_IN_CHUNK
          || encodeInfo.dataSizeInChunk >= TARGET_CHUNK_SIZE) {
        break;
      }
    }
  }

  @Override
  public TsBlock getBatch(int tsBlockIndex) {
    if (tsBlockIndex < 0 || tsBlockIndex >= tsBlocks.size()) {
      return null;
    }
    return tsBlocks.get(tsBlockIndex);
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  @Override
  public void close() throws IOException {
    tsBlocks.clear();
  }

  protected abstract void prepareNext();

  protected abstract void next();
}
