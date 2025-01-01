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

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.TVLIST_SORT_ALGORITHM;
import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public abstract class BinaryTVList extends TVList {
  // list of primitive array, add 1 when expanded -> Binary primitive array
  // index relation: arrayIndex -> elementIndex
  protected List<Binary[]> values;

  BinaryTVList() {
    super();
    values = new ArrayList<>();
  }

  public static BinaryTVList newList() {
    switch (TVLIST_SORT_ALGORITHM) {
      case QUICK:
        return new QuickBinaryTVList();
      case BACKWARD:
        return new BackBinaryTVList();
      default:
        return new TimBinaryTVList();
    }
  }

  @Override
  public TimBinaryTVList clone() {
    TimBinaryTVList cloneList = new TimBinaryTVList();
    cloneAs(cloneList);
    cloneSlicesAndBitMap(cloneList);
    for (Binary[] valueArray : values) {
      cloneList.values.add(cloneValue(valueArray));
    }
    return cloneList;
  }

  private Binary[] cloneValue(Binary[] array) {
    Binary[] cloneArray = new Binary[array.length];
    System.arraycopy(array, 0, cloneArray, 0, array.length);
    return cloneArray;
  }

  @Override
  public synchronized void putBinary(long timestamp, Binary value) {
    checkExpansion();
    int arrayIndex = rowCount / ARRAY_SIZE;
    int elementIndex = rowCount % ARRAY_SIZE;
    maxTime = Math.max(maxTime, timestamp);
    minTime = Math.min(minTime, timestamp);
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    indices.get(arrayIndex)[elementIndex] = rowCount;
    rowCount++;
    if (sorted) {
      if (rowCount > 1 && timestamp < getTime(rowCount - 2)) {
        sorted = false;
      } else {
        seqRowCount++;
      }
    }
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int newSize = 0;
    maxTime = Long.MIN_VALUE;
    for (int i = 0; i < rowCount; i++) {
      long time = getTime(i);
      if (time < lowerBound || time > upperBound) {
        set(i, newSize++);
        maxTime = Math.max(maxTime, time);
      }
    }
    int deletedNumber = rowCount - newSize;
    rowCount = newSize;
    // release primitive arrays that are empty
    int newArrayNum = newSize / ARRAY_SIZE;
    if (newSize % ARRAY_SIZE != 0) {
      newArrayNum++;
    }
    int oldArrayNum = timestamps.size();
    for (int releaseIdx = newArrayNum; releaseIdx < oldArrayNum; releaseIdx++) {
      releaseLastTimeArray();
      releaseLastValueArray();
    }
    return deletedNumber;
  }

  @Override
  public Binary getBinary(int index) {
    if (index >= rowCount) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    int valueIndex = getValueIndex(index);
    int arrayIndex = valueIndex / ARRAY_SIZE;
    int elementIndex = valueIndex % ARRAY_SIZE;
    return values.get(arrayIndex)[elementIndex];
  }

  @Override
  void clearValue() {
    if (values != null) {
      for (Binary[] dataArray : values) {
        PrimitiveArrayManager.release(dataArray);
      }
      values.clear();
    }
    clearSlicesAndBitMap();
  }

  @Override
  protected void expandValues() {
    values.add((Binary[]) getPrimitiveArraysByType(TSDataType.TEXT));
    expandSlicesAndBitMap();
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    return new TimeValuePair(
        getTime(index), TsPrimitiveType.getByType(TSDataType.TEXT, getBinary(index)));
  }

  @Override
  protected TimeValuePair getTimeValuePair(
      int index, long time, Integer floatPrecision, TSEncoding encoding) {
    return new TimeValuePair(time, TsPrimitiveType.getByType(TSDataType.TEXT, getBinary(index)));
  }

  @Override
  protected void writeValidValuesIntoTsBlock(
      TsBlockBuilder builder,
      int floatPrecision,
      TSEncoding encoding,
      List<TimeRange> deletionList) {
    int[] deleteCursor = {0};
    for (int i = 0; i < rowCount; i++) {
      if (!isNullValue(i)
          && !isPointDeleted(getTime(i), deletionList, deleteCursor)
          && (i == rowCount - 1 || getTime(i) != getTime(i + 1))) {
        builder.getTimeColumnBuilder().writeLong(getTime(i));
        builder.getColumnBuilder(0).writeBinary(getBinary(i));
        builder.declarePosition();
      }
    }
  }

  @Override
  protected void releaseLastValueArray() {
    PrimitiveArrayManager.release(values.remove(values.size() - 1));
  }

  @Override
  public synchronized void putBinaries(
      long[] time, Binary[] value, BitMap bitMap, int start, int end) {
    checkExpansion();

    int idx = start;
    // constraint: time.length + timeIdxOffset == value.length
    int timeIdxOffset = 0;
    if (bitMap != null && !bitMap.isAllUnmarked()) {
      // time array is a reference, should clone necessary time values
      long[] clonedTime = new long[end - start];
      System.arraycopy(time, start, clonedTime, 0, end - start);
      time = clonedTime;
      timeIdxOffset = start;
      // drop null at the end of value array
      int nullCnt =
          dropNullValThenUpdateMinMaxTimeAndSorted(time, value, bitMap, start, end, timeIdxOffset);
      end -= nullCnt;
    } else {
      updateMinMaxTimeAndSorted(time, start, end);
    }

    while (idx < end) {
      int inputRemaining = end - idx;
      int arrayIdx = rowCount / ARRAY_SIZE;
      int elementIdx = rowCount % ARRAY_SIZE;
      int internalRemaining = ARRAY_SIZE - elementIdx;
      if (internalRemaining >= inputRemaining) {
        // the remaining inputs can fit the last array, copy all remaining inputs into last array
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, inputRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, inputRemaining);
        int[] indexes = IntStream.range(rowCount, rowCount + inputRemaining).toArray();
        System.arraycopy(indexes, 0, indices.get(arrayIdx), elementIdx, inputRemaining);
        rowCount += inputRemaining;
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(
            time, idx - timeIdxOffset, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, internalRemaining);
        int[] indexes = IntStream.range(rowCount, rowCount + internalRemaining).toArray();
        System.arraycopy(indexes, 0, indices.get(arrayIdx), elementIdx, internalRemaining);
        idx += internalRemaining;
        rowCount += internalRemaining;
        checkExpansion();
      }
    }
  }

  // move null values to the end of time array and value array, then return number of null values
  int dropNullValThenUpdateMinMaxTimeAndSorted(
      long[] time, Binary[] values, BitMap bitMap, int start, int end, int tIdxOffset) {
    long inPutMinTime = Long.MAX_VALUE;
    boolean inputSorted = true;

    int nullCnt = 0;
    int inputSeqRowCount = 0;
    for (int vIdx = start; vIdx < end; vIdx++) {
      if (bitMap.isMarked(vIdx)) {
        nullCnt++;
        continue;
      }
      // move value ahead to replace null
      int tIdx = vIdx - tIdxOffset;
      if (nullCnt != 0) {
        time[tIdx - nullCnt] = time[tIdx];
        values[vIdx - nullCnt] = values[vIdx];
      }
      // update maxTime and sorted
      tIdx = tIdx - nullCnt;
      inPutMinTime = Math.min(inPutMinTime, time[tIdx]);
      maxTime = Math.max(maxTime, time[tIdx]);
      minTime = Math.min(minTime, time[tIdx]);
      if (inputSorted) {
        if (tIdx > 0 && time[tIdx - 1] > time[tIdx]) {
          inputSorted = false;
        } else {
          inputSeqRowCount++;
        }
      }
    }

    if (sorted
        && (rowCount == 0
            || (end - start > nullCnt) && time[start - tIdxOffset] > getTime(rowCount - 1))) {
      seqRowCount += inputSeqRowCount;
    }
    sorted = sorted && inputSorted && (rowCount == 0 || inPutMinTime >= getTime(rowCount - 1));
    return nullCnt;
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.TEXT;
  }

  @Override
  public int serializedSize() {
    int size = Byte.BYTES + Integer.BYTES + rowCount * (Long.BYTES + Byte.BYTES);
    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      size += ReadWriteIOUtils.sizeToWrite(getBinary(rowIdx));
    }
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(TSDataType.TEXT, buffer);
    buffer.putInt(rowCount);
    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      buffer.putLong(getTime(rowIdx));
      Binary valueT = getBinary(rowIdx);
      if (valueT != null) {
        WALWriteUtils.write(getBinary(rowIdx), buffer);
      } else {
        WALWriteUtils.write(new Binary(new byte[0]), buffer);
      }
      WALWriteUtils.write(isNullValue(rowIdx), buffer);
    }
  }

  public static BinaryTVList deserialize(DataInputStream stream) throws IOException {
    BinaryTVList tvList = BinaryTVList.newList();
    int rowCount = stream.readInt();
    long[] times = new long[rowCount];
    Binary[] values = new Binary[rowCount];
    BitMap bitMap = new BitMap(rowCount);
    for (int rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
      times[rowIdx] = stream.readLong();
      values[rowIdx] = ReadWriteIOUtils.readBinary(stream);
      if (ReadWriteIOUtils.readBool(stream)) {
        bitMap.mark(rowIdx);
      }
    }
    tvList.putBinaries(times, values, bitMap, 0, rowCount);
    return tvList;
  }
}
