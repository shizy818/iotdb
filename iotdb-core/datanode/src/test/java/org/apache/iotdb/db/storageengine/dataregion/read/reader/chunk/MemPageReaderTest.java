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
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.IntTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.tsfile.read.filter.factory.ValueFilterApi.DEFAULT_MEASUREMENT_INDEX;

public class MemPageReaderTest {

  private static final ReadOnlyMemChunk readOnlyMemChunk;
  private static final ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

  static {
    Statistics statistics = Statistics.getStatsByType(TSDataType.INT32);

    TSDataType dataType = TSDataType.INT32;
    String measurementId = "s1";
    IntTVList tvList = IntTVList.newList();
    for (int i = 0; i < 1000; i++) {
      tvList.putInt(i, i);
      statistics.update(i, i);
    }
    tvList.sort();

    try {
      List<TVList> sortedLists = new ArrayList<>();
      sortedLists.add(tvList);
      readOnlyMemChunk =
          new ReadOnlyMemChunk(
              new QueryContext(),
              measurementId,
              dataType,
              TSEncoding.PLAIN,
              sortedLists,
              null,
              null);
    } catch (IOException | QueryProcessException e) {
      throw new RuntimeException(e);
    }

    Mockito.when(chunkMetadata.getTimeStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata.getMeasurementStatistics(0)).thenReturn(Optional.of(statistics));
    Mockito.when(chunkMetadata.getDataType()).thenReturn(TSDataType.INT32);
  }

  private MemPageReader generatePageReader() {
    return new MemPageReader(readOnlyMemChunk.getPointReader(), chunkMetadata, null);
  }

  @Test
  public void testNullFilter() throws IOException {
    IPageReader pageReader = generatePageReader();

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(100, tsBlock.getPositionCount());
  }

  @Test
  public void testNullFilterAndLimitOffset() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.setLimitOffset(new PaginationController(10, 10));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(10, tsBlock.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock.getTimeByIndex(9));
  }

  @Test
  public void testFilterAllSatisfy() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(0));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(100, tsBlock.getPositionCount());
  }

  @Test
  public void testFilterAllSatisfyAndLimitOffset() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(0));
    pageReader.setLimitOffset(new PaginationController(10, 10));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(10, tsBlock.getTimeByIndex(0));
    Assert.assertEquals(19, tsBlock.getTimeByIndex(9));
  }

  @Test
  public void testFilter() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(50));
    pageReader.addRecordFilter(ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(30, tsBlock.getPositionCount());
  }

  @Test
  public void testFilterAndLimitOffset() throws IOException {
    IPageReader pageReader = generatePageReader();
    pageReader.addRecordFilter(TimeFilterApi.gtEq(50));
    pageReader.addRecordFilter(ValueFilterApi.lt(DEFAULT_MEASUREMENT_INDEX, 80, TSDataType.INT32));
    pageReader.setLimitOffset(new PaginationController(10, 10));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();

    Assert.assertEquals(10, tsBlock.getPositionCount());
    Assert.assertEquals(60, tsBlock.getTimeByIndex(0));
    Assert.assertEquals(69, tsBlock.getTimeByIndex(9));
  }
}
