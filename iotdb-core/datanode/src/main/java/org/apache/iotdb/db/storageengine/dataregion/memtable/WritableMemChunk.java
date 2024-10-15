/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class WritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  private TVList list;
  private List<TVList> sortedLists;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public WritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVList.newList(schema.getType());
    this.sortedLists = new ArrayList<>();
  }

  private WritableMemChunk() {}

  @Override
  public boolean writeWithFlushCheck(long insertTime, Object objectValue) {
    boolean shouldFlush;
    switch (schema.getType()) {
      case BOOLEAN:
        shouldFlush = putBooleanWithFlushCheck(insertTime, (boolean) objectValue);
        break;
      case INT32:
      case DATE:
        shouldFlush = putIntWithFlushCheck(insertTime, (int) objectValue);
        break;
      case INT64:
      case TIMESTAMP:
        shouldFlush = putLongWithFlushCheck(insertTime, (long) objectValue);
        break;
      case FLOAT:
        shouldFlush = putFloatWithFlushCheck(insertTime, (float) objectValue);
        break;
      case DOUBLE:
        shouldFlush = putDoubleWithFlushCheck(insertTime, (double) objectValue);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        shouldFlush = putBinaryWithFlushCheck(insertTime, (Binary) objectValue);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType().name());
    }
    if (shouldFlush) {
      return true;
    }

    if (list.rowCount() >= SORT_THRESHOLD) {
      sortTVList();
      sortedLists.add(list);
      this.list = TVList.newList(schema.getType());
    }
    return false;
  }

  @Override
  public boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    boolean shouldFlush;
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        shouldFlush = putBooleansWithFlushCheck(times, boolValues, bitMap, start, end);
        break;
      case INT32:
      case DATE:
        int[] intValues = (int[]) valueList;
        shouldFlush = putIntsWithFlushCheck(times, intValues, bitMap, start, end);
        break;
      case INT64:
      case TIMESTAMP:
        long[] longValues = (long[]) valueList;
        shouldFlush = putLongsWithFlushCheck(times, longValues, bitMap, start, end);
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        shouldFlush = putFloatsWithFlushCheck(times, floatValues, bitMap, start, end);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        shouldFlush = putDoublesWithFlushCheck(times, doubleValues, bitMap, start, end);
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary[] binaryValues = (Binary[]) valueList;
        shouldFlush = putBinariesWithFlushCheck(times, binaryValues, bitMap, start, end);
        break;
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType.name());
    }
    if (shouldFlush) {
      return true;
    }

    if (list.rowCount() >= SORT_THRESHOLD) {
      sortTVList();
      sortedLists.add(list);
      this.list = TVList.newList(schema.getType());
    }
    return false;
  }

  @Override
  public boolean writeAlignedValuesWithFlushCheck(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end,
      TSStatus[] results) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public boolean putLongWithFlushCheck(long t, long v) {
    list.putLong(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putIntWithFlushCheck(long t, int v) {
    list.putInt(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putFloatWithFlushCheck(long t, float v) {
    list.putFloat(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putDoubleWithFlushCheck(long t, double v) {
    list.putDouble(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBinaryWithFlushCheck(long t, Binary v) {
    list.putBinary(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBooleanWithFlushCheck(long t, boolean v) {
    list.putBoolean(t, v);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putAlignedValueWithFlushCheck(long t, Object[] v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public boolean putLongsWithFlushCheck(long[] t, long[] v, BitMap bitMap, int start, int end) {
    list.putLongs(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putIntsWithFlushCheck(long[] t, int[] v, BitMap bitMap, int start, int end) {
    list.putInts(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putFloatsWithFlushCheck(long[] t, float[] v, BitMap bitMap, int start, int end) {
    list.putFloats(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putDoublesWithFlushCheck(long[] t, double[] v, BitMap bitMap, int start, int end) {
    list.putDoubles(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBinariesWithFlushCheck(
      long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    list.putBinaries(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putBooleansWithFlushCheck(
      long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    list.putBooleans(t, v, bitMap, start, end);
    return list.reachChunkSizeOrPointNumThreshold();
  }

  @Override
  public boolean putAlignedValuesWithFlushCheck(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  private void sortTVList() {
    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public synchronized void sortTvListForFlush() {
    sortTVList();
  }

  @Override
  public long count() {
    long count = list.rowCount();
    for (TVList sortedList : sortedLists) {
      count += sortedList.rowCount();
    }
    return count;
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TVList getSortedTvListForQuery() {
    return null;
  }

  @Override
  public TVList getSortedTvListForQuery(List<IMeasurementSchema> schemaList, boolean ignoreAllNullRows) {
    return null;
  }

  @Override
  public long getMaxTime() {
    long maxTime = list.getMaxTime();
    for (TVList sortedList : sortedLists) {
      maxTime = Math.max(maxTime, sortedList.getMaxTime());
    }
    return maxTime;
  }

  private TimeValuePair first() {
    TimeValuePair tvPair = null;
    long time = Long.MAX_VALUE;
    for (TVList sortedList : sortedLists) {
      if (sortedList.rowCount() > 0 && sortedList.getTime(0) <= time) {
        tvPair = sortedList.getTimeValuePair(0);
        time = tvPair.getTimestamp();
      }
    }
    TVList clonedList = cloneAndSortList();
    if (clonedList.rowCount() > 0 && clonedList.getTime(0) <= time) {
      tvPair = clonedList.getTimeValuePair(0);
    }
    return tvPair;
  }

  @Override
  public long getFirstPoint() {
    TimeValuePair tvPair = first();
    if (tvPair == null) {
      return Long.MAX_VALUE;
    }
    return tvPair.getTimestamp();
  }

  private TimeValuePair last() {
    TimeValuePair tvPair = null;
    long time = Long.MIN_VALUE;
    for (TVList sortedList : sortedLists) {
      if (sortedList.rowCount() > 0 && sortedList.getTime(sortedList.rowCount() - 1) >= time) {
        tvPair = sortedList.getTimeValuePair(sortedList.rowCount() - 1);
        time = tvPair.getTimestamp();
      }
    }
    TVList clonedList = cloneAndSortList();
    if (clonedList.rowCount() > 0 && clonedList.getTime(clonedList.rowCount() - 1) >= time) {
      tvPair = clonedList.getTimeValuePair(clonedList.rowCount() - 1);
    }
    return tvPair;
  }

  @Override
  public long getLastPoint() {
    TimeValuePair tvPair = last();
    if (tvPair == null) {
      return Long.MIN_VALUE;
    }
    return tvPair.getTimestamp();
  }

  @Override
  public boolean isEmpty() {
    return count() == 0;
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = list.delete(lowerBound, upperBound);
    for (TVList sortedList : sortedLists) {
      deletedNumber += sortedList.delete(lowerBound, upperBound);
    }
    return deletedNumber;
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new ChunkWriterImpl(schema);
  }

  @Override
  public String toString() {
    StringBuilder out = new StringBuilder("MemChunk Size: " + count() + System.lineSeparator());
    if (count() > 0) {
      out.append("Data type:").append(schema.getType()).append(System.lineSeparator());
      out.append("First point:").append(first()).append(System.lineSeparator());
      out.append("Last point:").append(last()).append(System.lineSeparator());
    }
    return out.toString();
  }

  private void writeData(ChunkWriterImpl chunkWriterImpl, TimeValuePair tvPair) {
    switch (schema.getType()) {
      case BOOLEAN:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getBoolean());
        break;
      case INT32:
      case DATE:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getInt());
        break;
      case INT64:
      case TIMESTAMP:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getLong());
        break;
      case FLOAT:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getFloat());
        break;
      case DOUBLE:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getDouble());
        break;
      case TEXT:
      case BLOB:
      case STRING:
        chunkWriterImpl.write(tvPair.getTimestamp(), tvPair.getValue().getBinary());
        break;
      default:
        LOGGER.error("WritableMemChunk does not support data type: {}", schema.getType());
        break;
    }
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {
    ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;

    // ensure that list is sorted
    if (!list.isSorted()) {
      list.sort();
    }

    // acquire list iterator & sortedLists iterators
    List<TVList.TVListIterator> listIterators = new ArrayList<>();
    for (TVList sortedList : sortedLists) {
      listIterators.add(sortedList.iterator());
    }
    listIterators.add(list.iterator());

    TimeValuePair prevTvPair = null;
    for (int sortedRowIndex = 0; sortedRowIndex < count(); sortedRowIndex++) {
      // find next TimeValuePair from list & sortedLists
      int selectedTVListIndex = -1;
      long time = Long.MAX_VALUE;
      for (int i = 0; i < listIterators.size(); i++) {
        TimeValuePair tvPair = listIterators.get(i).current();
        if (tvPair != null && tvPair.getTimestamp() <= time) {
          time = tvPair.getTimestamp();
          selectedTVListIndex = i;
        }
      }
      TimeValuePair currentTvPair = listIterators.get(selectedTVListIndex).next();

      // skip duplicated data
      if (prevTvPair == null || prevTvPair.getTimestamp() == time) {
        prevTvPair = currentTvPair;
        continue;
      }

      writeData(chunkWriterImpl, prevTvPair);
      prevTvPair = currentTvPair;
    }

    // last point for SDT
    if (prevTvPair != null) {
      chunkWriterImpl.setLastPoint(true);
      writeData(chunkWriterImpl, prevTvPair);
    }
  }

  @Override
  public void release() {
    if (list.getReferenceCount() == 0) {
      list.clear();
    }
    for (TVList sortedList : sortedLists) {
      sortedList.clear();
    }
  }

  @Override
  public int serializedSize() {
    int serializedSize = schema.serializedSize() + list.serializedSize();
    for (TVList sortedList : sortedLists) {
      serializedSize += sortedList.serializedSize();
    }
    return serializedSize;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    byte[] bytes = new byte[schema.serializedSize()];
    schema.serializeTo(ByteBuffer.wrap(bytes));
    buffer.put(bytes);

    for (TVList sortedList : sortedLists) {
      sortedList.serializeToWAL(buffer);
    }
    list.serializeToWAL(buffer);
  }

  public static WritableMemChunk deserialize(DataInputStream stream) throws IOException {
    WritableMemChunk memChunk = new WritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    memChunk.list = TVList.deserialize(stream);
    return memChunk;
  }

  public TSDataType getDataType() {
    return schema.getType();
  }

  public boolean isSorted() {
    return list.isSorted();
  }

  public synchronized TVList cloneAndSortList() {
    TVList clonedList = list.clone();
    clonedList.sort();
    return clonedList;
  }

  public List<TVList> getSortedLists() {
    return sortedLists;
  }
}
