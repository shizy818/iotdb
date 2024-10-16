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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALWriteUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.chunk.IChunkWriter;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AlignedWritableMemChunk implements IWritableMemChunk {

  private final Map<String, Integer> measurementIndexMap;
  private final List<IMeasurementSchema> schemaList;
  private AlignedTVList list;
  private final List<AlignedTVList> sortedLists;

  private static final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";

  public AlignedWritableMemChunk(List<IMeasurementSchema> schemaList) {
    this.measurementIndexMap = new LinkedHashMap<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementId(), i);
      dataTypeList.add(schemaList.get(i).getType());
    }
    this.list = AlignedTVList.newAlignedList(dataTypeList);
    this.sortedLists = new ArrayList<>();
  }

  private AlignedWritableMemChunk(List<IMeasurementSchema> schemaList, AlignedTVList list) {
    this.measurementIndexMap = new LinkedHashMap<>();
    this.schemaList = schemaList;
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementId(), i);
    }
    this.list = list;
    this.sortedLists = new ArrayList<>();
  }

  public Set<String> getAllMeasurements() {
    return measurementIndexMap.keySet();
  }

  public boolean containsMeasurement(String measurementId) {
    return measurementIndexMap.containsKey(measurementId);
  }

  public int getMeasurementIndex(String measurementId) {
    return measurementIndexMap.getOrDefault(measurementId, -1);
  }

  @Override
  public boolean putLongWithFlushCheck(long t, long v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putIntWithFlushCheck(long t, int v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putFloatWithFlushCheck(long t, float v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putDoubleWithFlushCheck(long t, double v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBinaryWithFlushCheck(long t, Binary v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBooleanWithFlushCheck(long t, boolean v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putAlignedValueWithFlushCheck(long t, Object[] v) {
    list.putAlignedValue(t, v);
    if (list.reachChunkSizeOrPointNumThreshold()) {
      return true;
    }
    if (list.rowCount() >= SORT_THRESHOLD) {
      sortTVList();
      sortedLists.add(list);
      list = AlignedTVList.newAlignedList(getTsDataTypes());
    }
    return false;
  }

  @Override
  public boolean putLongsWithFlushCheck(long[] t, long[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putIntsWithFlushCheck(long[] t, int[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putFloatsWithFlushCheck(long[] t, float[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putDoublesWithFlushCheck(long[] t, double[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBinariesWithFlushCheck(
      long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putBooleansWithFlushCheck(
      long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean putAlignedValuesWithFlushCheck(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end, TSStatus[] results) {
    list.putAlignedValues(t, v, bitMaps, start, end, results);
    if (list.reachChunkSizeOrPointNumThreshold()) {
      return true;
    }
    if (list.rowCount() >= SORT_THRESHOLD) {
      sortTVList();
      sortedLists.add(list);
      list = AlignedTVList.newAlignedList(getTsDataTypes());
    }
    return false;
  }

  @Override
  public boolean writeWithFlushCheck(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + TSDataType.VECTOR);
  }

  @Override
  public boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    Object[] reorderedValue =
        checkAndReorderColumnValuesInInsertPlan(schemaList, objectValue, null).left;
    return putAlignedValueWithFlushCheck(insertTime, reorderedValue);
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
    Pair<Object[], BitMap[]> pair =
        checkAndReorderColumnValuesInInsertPlan(schemaList, valueList, bitMaps);
    Object[] reorderedColumnValues = pair.left;
    BitMap[] reorderedBitMaps = pair.right;
    return putAlignedValuesWithFlushCheck(
        times, reorderedColumnValues, reorderedBitMaps, start, end, results);
  }

  /**
   * Check metadata of columns and return array that mapping existed metadata to index of data
   * column.
   *
   * @param schemaListInInsertPlan Contains all existed schema in InsertPlan. If some timeseries
   *     have been deleted, there will be null in its slot.
   * @return columnIndexArray: schemaList[i] is schema of columns[columnIndexArray[i]]
   */
  private Pair<Object[], BitMap[]> checkAndReorderColumnValuesInInsertPlan(
      List<IMeasurementSchema> schemaListInInsertPlan, Object[] columnValues, BitMap[] bitMaps) {
    Object[] reorderedColumnValues = new Object[schemaList.size()];
    BitMap[] reorderedBitMaps = bitMaps == null ? null : new BitMap[schemaList.size()];
    for (int i = 0; i < schemaListInInsertPlan.size(); i++) {
      IMeasurementSchema measurementSchema = schemaListInInsertPlan.get(i);
      if (measurementSchema != null) {
        Integer index = this.measurementIndexMap.get(measurementSchema.getMeasurementId());
        // Index is null means this measurement was not in this AlignedTVList before.
        // We need to extend a new column in AlignedMemChunk and AlignedTVList.
        // And the reorderedColumnValues should extend one more column for the new measurement
        if (index == null) {
          index = measurementIndexMap.size();
          this.measurementIndexMap.put(schemaListInInsertPlan.get(i).getMeasurementId(), index);
          this.schemaList.add(schemaListInInsertPlan.get(i));
          this.list.extendColumn(schemaListInInsertPlan.get(i).getType());
          reorderedColumnValues =
              Arrays.copyOf(reorderedColumnValues, reorderedColumnValues.length + 1);
          if (reorderedBitMaps != null) {
            reorderedBitMaps = Arrays.copyOf(reorderedBitMaps, reorderedBitMaps.length + 1);
          }
        }
        reorderedColumnValues[index] = columnValues[i];
        if (bitMaps != null) {
          reorderedBitMaps[index] = bitMaps[i];
        }
      }
    }
    return new Pair<>(reorderedColumnValues, reorderedBitMaps);
  }

  @Override
  public long count() {
    return (long) alignedListSize() * measurementIndexMap.size();
  }

  public int alignedListSize() {
    int count = list.rowCount();
    for (TVList sortedList : sortedLists) {
      count += sortedList.rowCount();
    }
    return count;
  }

  @Override
  public IMeasurementSchema getSchema() {
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

  private void sortTVList() {
    // check reference count
    if ((list.getReferenceCount() > 0 && !list.isSorted())) {
      list = list.clone();
    }

    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public synchronized void sortTvListForFlush() {
    sortTVList();
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    int deletedNumber = list.delete(lowerBound, upperBound);
    for (TVList sortedList : sortedLists) {
      deletedNumber += sortedList.delete(lowerBound, upperBound);
    }
    return deletedNumber;
  }

  public Pair<Integer, Boolean> deleteDataFromAColumn(
      long lowerBound, long upperBound, String measurementId) {
    Pair<Integer, Boolean> deletePair =
        list.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
    for (AlignedTVList sortedList : sortedLists) {
      Pair<Integer, Boolean> p =
          sortedList.delete(lowerBound, upperBound, measurementIndexMap.get(measurementId));
      deletePair.left += p.left;
      deletePair.right = deletePair.right && p.right;
    }
    return deletePair;
  }

  public void removeColumn(String measurementId) {
    list.deleteColumn(measurementIndexMap.get(measurementId));
    for (AlignedTVList sortedList : sortedLists) {
      sortedList.deleteColumn(measurementIndexMap.get(measurementId));
    }
    IMeasurementSchema schemaToBeRemoved = schemaList.get(measurementIndexMap.get(measurementId));
    schemaList.remove(schemaToBeRemoved);
    measurementIndexMap.clear();
    for (int i = 0; i < schemaList.size(); i++) {
      measurementIndexMap.put(schemaList.get(i).getMeasurementId(), i);
    }
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schemaList);
  }

  private int findListIteratorIndex(List<AlignedTVList.AlignedTVListIterator> listIterators) {
    int selectedTVListIndex = -1;
    long time = Long.MAX_VALUE;
    for (int i = 0; i < listIterators.size(); i++) {
      AlignedTVList.AlignedTVListIterator iterator = listIterators.get(i);
      TimeValuePair tvPair = iterator.current();
      if (tvPair != null && tvPair.getTimestamp() <= time) {
        time = tvPair.getTimestamp();
        selectedTVListIndex = i;
      }
    }
    return selectedTVListIndex;
  }

  private void writePage(
      IChunkWriter chunkWriter, long[] times, List<Object[]> values, int pointsInPage) {
    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriter;
    List<TSDataType> dataTypes = getTsDataTypes();
    for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
      TSDataType tsDataType = dataTypes.get(columnIndex);
      for (int recordIndex = 0; recordIndex < pointsInPage; recordIndex++) {
        long time = times[recordIndex];
        Object value = values.get(columnIndex)[recordIndex];
        boolean isNull = (value == null);
        switch (tsDataType) {
          case BOOLEAN:
            alignedChunkWriter.writeByColumn(time, (boolean) value, isNull);
            break;
          case INT32:
            alignedChunkWriter.writeByColumn(time, (int) value, isNull);
            break;
          case INT64:
            alignedChunkWriter.writeByColumn(time, (long) value, isNull);
            break;
          case FLOAT:
            alignedChunkWriter.writeByColumn(time, (float) value, isNull);
            break;
          case DOUBLE:
            alignedChunkWriter.writeByColumn(time, (double) value, isNull);
            break;
          case TEXT:
            alignedChunkWriter.writeByColumn(time, (Binary) value, isNull);
            break;
          default:
            break;
        }
      }
      alignedChunkWriter.nextColumn();
    }
    alignedChunkWriter.write(times, pointsInPage, 0);
  }

  private TimeValuePair extendAlignedColumn(TimeValuePair tvp, int columnSize) {
    if (tvp.getValue().getDataType() != TSDataType.VECTOR) {
      throw new UnsupportedOperationException("cannot extend non vector time-value pair");
    }
    TsPrimitiveType[] oldValues = tvp.getValue().getVector();
    TsPrimitiveType[] newValues = new TsPrimitiveType[columnSize];
    System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);
    return new TimeValuePair(tvp.getTimestamp(), new TsPrimitiveType.TsVector(newValues));
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {
    // ensure that list is sorted
    if (!list.isSorted()) {
      list.sort();
    }

    // list iterator & sortedLists iterators
    int validRowCount = 0;
    List<AlignedTVList.AlignedTVListIterator> listIterators = new ArrayList<>();
    for (AlignedTVList sortedList : sortedLists) {
      validRowCount += sortedList.validRowCount();
      listIterators.add(sortedList.iterator(true));
    }
    validRowCount += list.validRowCount();
    listIterators.add(list.iterator(true));

    // datatype & time & values list
    List<TSDataType> dataTypes = getTsDataTypes();
    long[] times = new long[MAX_NUMBER_OF_POINTS_IN_PAGE];
    List<Object[]> values = new ArrayList<>();
    for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
      values.add(new Object[MAX_NUMBER_OF_POINTS_IN_PAGE]);
    }
    int pointsInPages = 0;

    Object[] lastValidValueForTimeDup = null;
    TimeValuePair prevTvPair = null;
    for (int sortedRowIndex = 0; sortedRowIndex < validRowCount; sortedRowIndex++) {
      int selectedTVListIndex = findListIteratorIndex(listIterators);
      TimeValuePair currTvPair = listIterators.get(selectedTVListIndex).next();

      // It is possible that column number in list is more than that in sortedList.
      // In such case we add extra null values in the TimeValuePair.
      // for example:
      //    sortedLists[0]: [s1,s2]
      //    sortedLists[1]: [s1,s2,s3]
      //    list: [s1,s2,s3,s4]
      TsPrimitiveType[] currTvPairValues = currTvPair.getValue().getVector();
      if (currTvPairValues.length < getTsDataTypes().size()) {
        currTvPair = extendAlignedColumn(currTvPair, getTsDataTypes().size());
      }

      // skip first record
      if (prevTvPair == null) {
        prevTvPair = currTvPair;
        continue;
      }

      TsPrimitiveType[] currentValues = currTvPair.getValue().getVector();
      if (currTvPair.getTimestamp() == prevTvPair.getTimestamp()) {
        if (lastValidValueForTimeDup == null) {
          lastValidValueForTimeDup = new Object[dataTypes.size()];
        }
        for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
          if (currentValues[columnIndex] != null) {
            // update not-null column in lastValidValueForTimeDup
            lastValidValueForTimeDup[columnIndex] = currentValues[columnIndex];
          }
        }
      } else {
        for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
          values.get(columnIndex)[pointsInPages] =
              lastValidValueForTimeDup != null
                  ? lastValidValueForTimeDup[columnIndex]
                  : prevTvPair.getValue().getVector()[columnIndex];
        }
        times[pointsInPages] = prevTvPair.getTimestamp();
        lastValidValueForTimeDup = null;

        pointsInPages++;
      }
      prevTvPair = currTvPair;

      if (pointsInPages == MAX_NUMBER_OF_POINTS_IN_PAGE) {
        // chunkWriter
        writePage(chunkWriter, times, values, pointsInPages);
        // clear data
        pointsInPages = 0;
        Arrays.fill(times, 0);
        values.clear();
        for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
          values.add(new Object[MAX_NUMBER_OF_POINTS_IN_PAGE]);
        }
      }
    }

    // write last row
    if (prevTvPair != null) {
      for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
        values.get(columnIndex)[pointsInPages] =
            lastValidValueForTimeDup != null
                ? lastValidValueForTimeDup[columnIndex]
                : prevTvPair.getValue().getVector()[columnIndex];
      }
      times[pointsInPages] = prevTvPair.getTimestamp();
      pointsInPages++;
    }
    writePage(chunkWriter, times, values, pointsInPages);
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
  public long getFirstPoint() {
    if (alignedListSize() == 0) {
      return Long.MAX_VALUE;
    }

    AlignedTVList clonedList = cloneAndSortList();
    List<AlignedTVList.AlignedTVListIterator> iterators = new ArrayList<>();
    for (AlignedTVList sortedList : sortedLists) {
      iterators.add(sortedList.iterator(true));
    }
    iterators.add(clonedList.iterator(true));

    long time = Long.MAX_VALUE;
    for (AlignedTVList.AlignedTVListIterator iterator : iterators) {
      if (iterator.hasNext()) {
        TimeValuePair tvPair = iterator.next();
        if (tvPair.getTimestamp() <= time) {
          time = tvPair.getTimestamp();
        }
      }
    }
    return time;
  }

  @Override
  public long getLastPoint() {
    if (alignedListSize() == 0) {
      return Long.MIN_VALUE;
    }

    AlignedTVList clonedList = cloneAndSortList();
    List<AlignedTVList.AlignedTVListIterator> iterators = new ArrayList<>();
    for (AlignedTVList sortedList : sortedLists) {
      iterators.add(sortedList.iterator(true));
    }
    iterators.add(clonedList.iterator(true));

    long time = Long.MIN_VALUE;
    for (AlignedTVList.AlignedTVListIterator iterator : iterators) {
      iterator.seekToLast();
      if (iterator.hasPrevious()) {
        TimeValuePair tvPair = iterator.previous();
        if (tvPair.getTimestamp() >= time) {
          time = tvPair.getTimestamp();
        }
      }
    }
    return time;
  }

  @Override
  public boolean isEmpty() {
    return alignedListSize() == 0;
  }

  @Override
  public int serializedSize() {
    int size = 0;
    size += Integer.BYTES;
    for (IMeasurementSchema schema : schemaList) {
      size += schema.serializedSize();
    }

    for (TVList sortedList : sortedLists) {
      size += sortedList.serializedSize();
    }
    size += list.serializedSize();
    return size;
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(schemaList.size(), buffer);
    for (IMeasurementSchema schema : schemaList) {
      byte[] bytes = new byte[schema.serializedSize()];
      schema.serializeTo(ByteBuffer.wrap(bytes));
      buffer.put(bytes);
    }

    for (TVList sortedList : sortedLists) {
      sortedList.serializeToWAL(buffer);
    }
    list.serializeToWAL(buffer);
  }

  public static AlignedWritableMemChunk deserialize(DataInputStream stream) throws IOException {
    int schemaListSize = stream.readInt();
    List<IMeasurementSchema> schemaList = new ArrayList<>(schemaListSize);
    for (int i = 0; i < schemaListSize; i++) {
      IMeasurementSchema schema = MeasurementSchema.deserializeFrom(stream);
      schemaList.add(schema);
    }

    AlignedTVList list = (AlignedTVList) TVList.deserialize(stream);
    return new AlignedWritableMemChunk(schemaList, list);
  }

  public List<IMeasurementSchema> getSchemaList() {
    return schemaList;
  }

  public List<TSDataType> getTsDataTypes() {
    return schemaList.stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
  }

  public synchronized AlignedTVList cloneAndSortList() {
    AlignedTVList clonedList = list.clone();
    clonedList.sort();
    return clonedList;
  }

  public List<AlignedTVList> getSortedLists() {
    return sortedLists;
  }
}
