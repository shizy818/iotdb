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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.VectorMeasurementSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class AlignedMemPageReaderTest {

  private static final AlignedReadOnlyMemChunk alignedChunk1;
  private static final AlignedReadOnlyMemChunk alignedChunk2;
  private static final AlignedChunkMetadata chunkMetadata1 =
      Mockito.mock(AlignedChunkMetadata.class);

  private static final AlignedChunkMetadata chunkMetadata2 =
      Mockito.mock(AlignedChunkMetadata.class);
  private static final QueryContext ctx = Mockito.mock(QueryContext.class);

  static {
    TimeStatistics timeStatistics = new TimeStatistics();
    IntegerStatistics valueStatistics1 = new IntegerStatistics();
    IntegerStatistics valueStatistics2 = new IntegerStatistics();

    AlignedTVList list1 =
        AlignedTVList.newAlignedList(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    AlignedTVList list2 = AlignedTVList.newAlignedList(Collections.singletonList(TSDataType.INT32));

    // table model to return all null row
    Mockito.when(ctx.isIgnoreAllNullRows()).thenReturn(false);
    Mockito.when(chunkMetadata1.getTimeStatistics()).thenReturn((Statistics) timeStatistics);
    Mockito.when(chunkMetadata1.getMeasurementStatistics(0))
        .thenReturn(Optional.of(valueStatistics1));
    Mockito.when(chunkMetadata1.getMeasurementStatistics(1))
        .thenReturn(Optional.of(valueStatistics2));

    Mockito.when(chunkMetadata2.getTimeStatistics()).thenReturn((Statistics) timeStatistics);
    Mockito.when(chunkMetadata2.getMeasurementStatistics(0))
        .thenReturn(Optional.of(valueStatistics2));

    try {
      alignedChunk1 = buildAlignedChunk1(list1, timeStatistics, valueStatistics1);
      alignedChunk2 = buildAlignedChunk2(list2, timeStatistics, valueStatistics2);
    } catch (QueryProcessException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static AlignedReadOnlyMemChunk buildAlignedChunk1(
      AlignedTVList list, TimeStatistics timeStatistics, IntegerStatistics valueStatistics)
      throws QueryProcessException, IOException {
    for (int i = 0; i < 100; i++) {
      Object[] value = new Object[2];
      value[0] = i;
      if (i >= 10 && i < 90) {
        value[1] = i;
      }
      timeStatistics.update(i);
      valueStatistics.update(i, i);
      list.putAlignedValue(i, value);
    }

    List<AlignedTVList> sortedLists = new ArrayList<>();
    sortedLists.add(list);
    IMeasurementSchema schema =
        new VectorMeasurementSchema(
            "d1",
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT32},
            new TSEncoding[] {TSEncoding.PLAIN, TSEncoding.PLAIN},
            CompressionType.UNCOMPRESSED);

    return new AlignedReadOnlyMemChunk(ctx, schema, null, sortedLists, null, null);
  }

  private static AlignedReadOnlyMemChunk buildAlignedChunk2(
      AlignedTVList list, TimeStatistics timeStatistics, IntegerStatistics valueStatistics)
      throws QueryProcessException, IOException {
    for (int i = 0; i < 100; i++) {
      Object[] value = new Object[1];
      if (i >= 10 && i < 90) {
        value[0] = i;
        valueStatistics.update(i, i);
      }
      timeStatistics.update(i);
      list.putAlignedValue(i, value);
    }

    List<AlignedTVList> sortedLists = new ArrayList<>();
    sortedLists.add(list);
    IMeasurementSchema schema =
        new VectorMeasurementSchema(
            "d2",
            new String[] {"s3"},
            new TSDataType[] {TSDataType.INT32},
            new TSEncoding[] {TSEncoding.PLAIN},
            CompressionType.UNCOMPRESSED);

    return new AlignedReadOnlyMemChunk(ctx, schema, null, sortedLists, null, null);
  }

  private MemAlignedPageReader generateAlignedPageReader() {
    MemAlignedPageReader alignedPageReader =
        new MemAlignedPageReader(alignedChunk1.getPointReader(), chunkMetadata1, null);
    alignedPageReader.initTsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    return alignedPageReader;
  }

  private MemAlignedPageReader generateSingleColumnAlignedPageReader() {
    MemAlignedPageReader alignedPageReader =
        new MemAlignedPageReader(alignedChunk2.getPointReader(), chunkMetadata2, null);
    alignedPageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    return alignedPageReader;
  }

  @Test
  public void testNullFilter() throws IOException {
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(100, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    // AlignedTVListIterator skip rows when all column values are null
    Assert.assertEquals(100, tsBlock2.getPositionCount());
  }

  @Test
  public void testNullFilterWithLimitOffset() throws IOException {
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(10, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(10, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testGlobalTimeFilterAllSatisfy() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(0L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(40, tsBlock2.getPositionCount());
  }

  @Test
  public void testGlobalTimeFilterAllSatisfyWithLimitOffset() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(0L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 50, TSDataType.INT32));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testPushDownFilterAllSatisfy() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(40, tsBlock2.getPositionCount());
  }

  @Test
  public void testPushDownFilterAllSatisfyWithLimitOffset() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.gtEq(DEFAULT_MEASUREMENT_INDEX, 0, TSDataType.INT32));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }

  @Test
  public void testFilter() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(30L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock1.getPositionCount());

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(50, tsBlock2.getPositionCount());
  }

  @Test
  public void testFilterWithLimitOffset() throws IOException {
    Filter globalTimeFilter = TimeFilterApi.gtEq(50L);
    MemAlignedPageReader alignedPageReader1 = generateAlignedPageReader();
    alignedPageReader1.addRecordFilter(globalTimeFilter);
    alignedPageReader1.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    alignedPageReader1.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock1 = alignedPageReader1.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock1.getPositionCount());
    Assert.assertEquals(60, tsBlock1.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock1.getTimeByIndex(9));

    MemAlignedPageReader alignedPageReader2 = generateSingleColumnAlignedPageReader();
    alignedPageReader2.addRecordFilter(globalTimeFilter);
    alignedPageReader2.addRecordFilter(
        ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    alignedPageReader2.setLimitOffset(new PaginationController(10, 10));
    TsBlock tsBlock2 = alignedPageReader2.getAllSatisfiedData();
    Assert.assertEquals(10, tsBlock2.getPositionCount());
    Assert.assertEquals(60, tsBlock2.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock2.getTimeByIndex(9));
  }
}
