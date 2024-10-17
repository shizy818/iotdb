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

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.TVLIST_SORT_ALGORITHM;
import static org.apache.iotdb.db.utils.MemUtils.getBinarySize;
import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

public abstract class BinaryTVList extends TVList {
  // list of primitive array, add 1 when expanded -> Binary primitive array
  // index relation: arrayIndex -> elementIndex
  protected List<Binary[]> values;

  // record total memory size of binary tvlist
  long memoryBinaryChunkSize;

  BinaryTVList() {
    super();
    values = new ArrayList<>();
    memoryBinaryChunkSize = 0;
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
    cloneList.memoryBinaryChunkSize = memoryBinaryChunkSize;
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
  public void putBinary(long timestamp, Binary value) {
    checkExpansion();
    int arrayIndex = rowCount / ARRAY_SIZE;
    int elementIndex = rowCount % ARRAY_SIZE;
    maxTime = Math.max(maxTime, timestamp);
    timestamps.get(arrayIndex)[elementIndex] = timestamp;
    values.get(arrayIndex)[elementIndex] = value;
    indices.get(arrayIndex)[elementIndex] = rowCount;
    rowCount++;
    if (sorted && rowCount > 1 && timestamp < getTime(rowCount - 2)) {
      sorted = false;
    }
    memoryBinaryChunkSize += getBinarySize(value);
  }

  @Override
  public boolean reachChunkSizeOrPointNumThreshold() {
    return memoryBinaryChunkSize >= TARGET_CHUNK_SIZE || rowCount >= MAX_SERIES_POINT_NUMBER;
  }

  @Override
  public Binary getBinary(int index) {
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
    memoryBinaryChunkSize = 0;
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
  public void putBinaries(long[] time, Binary[] value, BitMap bitMap, int start, int end) {
    checkExpansion();

    int idx = start;
    updateMaxTimeAndSorted(time, start, end);

    // update raw size
    for (int i = idx; i < end; i++) {
      memoryBinaryChunkSize += getBinarySize(value[i]);
    }

    while (idx < end) {
      int inputRemaining = end - idx;
      int arrayIdx = rowCount / ARRAY_SIZE;
      int elementIdx = rowCount % ARRAY_SIZE;
      int internalRemaining = ARRAY_SIZE - elementIdx;
      if (internalRemaining >= inputRemaining) {
        // the remaining inputs can fit the last array, copy all remaining inputs into last array
        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, inputRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, inputRemaining);
        for (int i = 0; i < inputRemaining; i++) {
          indices.get(arrayIdx)[elementIdx + i] = rowCount;
          if (values == null || bitMap != null && bitMap.isMarked(idx + i)) {
            markNullValue(arrayIdx, elementIdx + i);
          }
          rowCount++;
        }
        break;
      } else {
        // the remaining inputs cannot fit the last array, fill the last array and create a new
        // one and enter the next loop
        System.arraycopy(time, idx, timestamps.get(arrayIdx), elementIdx, internalRemaining);
        System.arraycopy(value, idx, values.get(arrayIdx), elementIdx, internalRemaining);
        for (int i = 0; i < internalRemaining; i++) {
          indices.get(arrayIdx)[elementIdx + i] = rowCount;
          if (values == null || bitMap != null && bitMap.isMarked(idx + i)) {
            markNullValue(arrayIdx, elementIdx + i);
          }
          rowCount++;
        }
        idx += internalRemaining;
        checkExpansion();
      }
    }
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
