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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.db.utils.MathUtils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

public abstract class TVList implements WALEntryValue {
  protected static final String ERR_DATATYPE_NOT_CONSISTENT = "DataType not consistent";
  // list of timestamp array, add 1 when expanded -> data point timestamp array
  // index relation: arrayIndex -> elementIndex
  protected List<long[]> timestamps;
  protected int rowCount;

  // used by non-aligned TVList
  // Index relation: arrayIndex -> elementIndex
  protected List<BitMap> bitMap;

  protected boolean sorted = true;
  protected long maxTime;
  protected long minTime;
  // record reference count of this tv list
  // currently this reference will only be increase because we can't know when to decrease it
  protected AtomicInteger referenceCount;
  private long version;

  protected TVList() {
    timestamps = new ArrayList<>();
    rowCount = 0;
    maxTime = Long.MIN_VALUE;
    minTime = Long.MAX_VALUE;
    referenceCount = new AtomicInteger();
  }

  public static TVList newList(TSDataType dataType) {
    switch (dataType) {
      case TEXT:
      case BLOB:
      case STRING:
        return BinaryTVList.newList();
      case FLOAT:
        return FloatTVList.newList();
      case INT32:
      case DATE:
        return IntTVList.newList();
      case INT64:
      case TIMESTAMP:
        return LongTVList.newList();
      case DOUBLE:
        return DoubleTVList.newList();
      case BOOLEAN:
        return BooleanTVList.newList();
      default:
        break;
    }
    return null;
  }

  public static long tvListArrayMemCost(TSDataType type) {
    long size = 0;
    // time array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * 8L;
    // value array mem size
    size += PrimitiveArrayManager.ARRAY_SIZE * (long) type.getDataTypeSize();
    // two array headers mem size
    size += NUM_BYTES_ARRAY_HEADER * 2L;
    // Object references size in ArrayList
    size += NUM_BYTES_OBJECT_REF * 2L;
    return size;
  }

  public boolean isSorted() {
    return sorted;
  }

  public abstract void sort();

  public void increaseReferenceCount() {
    referenceCount.incrementAndGet();
  }

  public int getReferenceCount() {
    return referenceCount.get();
  }

  public int rowCount() {
    return rowCount;
  }

  public int count() {
    if (bitMap == null) {
      return rowCount;
    }
    int count = 0;
    for (int row = 0; row < rowCount; row++) {
      if (!isNullValue(row)) {
        count++;
      }
    }
    return count;
  }

  public long getTime(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int arrayIndex = index / ARRAY_SIZE;
    int elementIndex = index % ARRAY_SIZE;
    return timestamps.get(arrayIndex)[elementIndex];
  }

  public void putLong(long time, long value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInt(long time, int value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloat(long time, float value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDouble(long time, double value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinary(long time, Binary value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBoolean(long time, boolean value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putAlignedValue(long time, Object[] value) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putLongs(long[] time, long[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putInts(long[] time, int[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putFloats(long[] time, float[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putDoubles(long[] time, double[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBinaries(long[] time, Binary[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putBooleans(long[] time, boolean[] value, BitMap bitMap, int start, int end) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public void putAlignedValues(
      long[] time, Object[] value, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public long getLong(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public int getInt(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public float getFloat(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public double getDouble(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public Binary getBinary(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public Object getAlignedValue(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public TVList getTvListByColumnIndex(
      List<Integer> columnIndexList, List<TSDataType> dataTypeList, boolean ignoreAllNullRows) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public int getValueIndex(int index) {
    throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
  }

  public long getMaxTime() {
    return maxTime;
  }

  public long getMinTime() {
    return minTime;
  }

  public long getVersion() {
    return version;
  }

  protected abstract void set(int src, int dest);

  protected abstract void expandValues();

  @Override
  public abstract TVList clone();

  public TVList clone(long version) {
    this.version = version;
    return clone();
  }

  protected abstract void releaseLastValueArray();

  protected void releaseLastTimeArray() {
    PrimitiveArrayManager.release(timestamps.remove(timestamps.size() - 1));
  }

  // todo: binary search
  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = 0;
    long maxTime = Long.MIN_VALUE;
    long minTime = Long.MAX_VALUE;
    for (int i = 0; i < rowCount; i++) {
      long time = getTime(i);
      if (time >= lowerBound && time <= upperBound) {
        if (!isNullValue(i)) {
          int arrayIndex = i / ARRAY_SIZE;
          int elementIndex = i % ARRAY_SIZE;
          markNullValue(arrayIndex, elementIndex);
          deletedNumber++;
        }
      } else {
        maxTime = Math.max(time, maxTime);
        minTime = Math.min(time, minTime);
      }
    }
    return deletedNumber;
  }

  protected void cloneAs(TVList cloneList) {
    for (long[] timestampArray : timestamps) {
      cloneList.timestamps.add(cloneTime(timestampArray));
    }
    cloneList.rowCount = rowCount;
    cloneList.sorted = sorted;
    cloneList.maxTime = maxTime;
    cloneList.minTime = minTime;
  }

  public void clear() {
    rowCount = 0;
    sorted = true;
    maxTime = Long.MIN_VALUE;
    minTime = Long.MAX_VALUE;
    clearTime();
    clearValue();
    clearBitMap();
  }

  protected void clearTime() {
    if (timestamps != null) {
      for (long[] dataArray : timestamps) {
        PrimitiveArrayManager.release(dataArray);
      }
      timestamps.clear();
    }
  }

  abstract void clearValue();

  protected void checkExpansion() {
    if ((rowCount % ARRAY_SIZE) == 0) {
      expandValues();
      timestamps.add((long[]) getPrimitiveArraysByType(TSDataType.INT64));
    }
  }

  protected Object getPrimitiveArraysByType(TSDataType dataType) {
    return PrimitiveArrayManager.allocate(dataType);
  }

  protected long[] cloneTime(long[] array) {
    long[] cloneArray = new long[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  void updateMaxMinTimeAndSorted(long[] time, int start, int end) {
    int length = time.length;
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;
    for (int i = start; i < end; i++) {
      inPutMinTime = Math.min(inPutMinTime, time[i]);
      maxTime = Math.max(maxTime, time[i]);
      minTime = Math.min(minTime, time[i]);
      if (inputSorted && i < length - 1 && time[i] > time[i + 1]) {
        inputSorted = false;
      }
    }
    sorted = sorted && inputSorted && (rowCount == 0 || inPutMinTime >= getTime(rowCount - 1));
  }

  /** for log */
  public abstract TimeValuePair getTimeValuePair(int index);

  protected abstract TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding);

  @TestOnly
  public TsBlock buildTsBlock() {
    return buildTsBlock(0, TSEncoding.PLAIN, null);
  }

  public TsBlock buildTsBlock(
      int floatPrecision, TSEncoding encoding, List<TimeRange> deletionList) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(this.getDataType()));
    writeValidValuesIntoTsBlock(builder, floatPrecision, encoding, deletionList);
    return builder.build();
  }

  protected abstract void writeValidValuesIntoTsBlock(
      TsBlockBuilder builder,
      int floatPrecision,
      TSEncoding encoding,
      List<TimeRange> deletionList);

  protected float roundValueWithGivenPrecision(
      float value, int floatPrecision, TSEncoding encoding) {
    if (!Float.isNaN(value) && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
      return MathUtils.roundWithGivenPrecision(value, floatPrecision);
    }
    return value;
  }

  protected double roundValueWithGivenPrecision(
      double value, int floatPrecision, TSEncoding encoding) {
    if (!Double.isNaN(value) && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
      return MathUtils.roundWithGivenPrecision(value, floatPrecision);
    }
    return value;
  }

  public abstract TSDataType getDataType();

  public static TVList deserialize(DataInputStream stream) throws IOException {
    TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    switch (dataType) {
      case TEXT:
      case BLOB:
      case STRING:
        return BinaryTVList.deserialize(stream);
      case FLOAT:
        return FloatTVList.deserialize(stream);
      case INT32:
      case DATE:
        return IntTVList.deserialize(stream);
      case INT64:
      case TIMESTAMP:
        return LongTVList.deserialize(stream);
      case DOUBLE:
        return DoubleTVList.deserialize(stream);
      case BOOLEAN:
        return BooleanTVList.deserialize(stream);
      case VECTOR:
        return AlignedTVList.deserialize(stream);
      default:
        break;
    }
    return null;
  }

  public List<long[]> getTimestamps() {
    return timestamps;
  }

  public int binarySearch(long ts) {
    if (!sorted) {
      return -1;
    }
    int left = 0;
    int right = rowCount;
    while (left < right) {
      int mid = left + (right - left) / 2;
      if (getTime(mid) <= ts) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return right - 1;
  }

  public boolean isNullValue(int unsortedRowIndex) {
    if (unsortedRowIndex >= rowCount) {
      throw new IndexOutOfBoundsException("Index out of bound error!");
    }
    if (bitMap == null || bitMap.get(unsortedRowIndex / ARRAY_SIZE) == null) {
      return false;
    }
    int arrayIndex = unsortedRowIndex / ARRAY_SIZE;
    int elementIndex = unsortedRowIndex % ARRAY_SIZE;
    return bitMap.get(arrayIndex).isMarked(elementIndex);
  }

  public void markNullValue(int arrayIndex, int elementIndex) {
    // init bitMap if doesn't have
    if (bitMap == null) {
      bitMap = new ArrayList<>();
      for (int i = 0; i < timestamps.size(); i++) {
        bitMap.add(new BitMap(ARRAY_SIZE));
      }
    }
    // if the bitmap in arrayIndex is null, init the bitmap
    if (bitMap.get(arrayIndex) == null) {
      bitMap.set(arrayIndex, new BitMap(ARRAY_SIZE));
    }

    // mark the null value in the current bitmap
    bitMap.get(arrayIndex).mark(elementIndex);
  }

  protected void cloneBitMap(TVList cloneList) {
    if (bitMap != null) {
      cloneList.bitMap = new ArrayList<>();
      for (BitMap bm : bitMap) {
        cloneList.bitMap.add(bm == null ? null : bm.clone());
      }
    }
  }

  protected void clearBitMap() {
    if (bitMap != null) {
      bitMap.clear();
    }
  }
}
