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

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunkIterator;
import org.apache.iotdb.db.utils.datastructure.BinaryTVList;
import org.apache.iotdb.db.utils.datastructure.BooleanTVList;
import org.apache.iotdb.db.utils.datastructure.DoubleTVList;
import org.apache.iotdb.db.utils.datastructure.FloatTVList;
import org.apache.iotdb.db.utils.datastructure.IntTVList;
import org.apache.iotdb.db.utils.datastructure.LongTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MemChunkLoaderTest {

  private static final String BINARY_STR = "ty love zm";

  @Test
  public void testBooleanMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(buildBooleanPointReader());
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.BOOLEAN);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.BOOLEAN));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private IPointReader buildBooleanPointReader() {
    List<TVList> sortedTVLists = new ArrayList<>();
    BooleanTVList tvList = BooleanTVList.newList();
    tvList.putBoolean(1L, true);
    tvList.putBoolean(2L, false);
    sortedTVLists.add(tvList);

    return new ReadOnlyMemChunkIterator(sortedTVLists, null);
  }

  @Test
  public void testInt32MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(buildIntPointReader());
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.INT32);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.INT32));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private IPointReader buildIntPointReader() {
    List<TVList> sortedTVLists = new ArrayList<>();
    IntTVList tvList = IntTVList.newList();
    tvList.putInt(1L, 1);
    tvList.putInt(2L, 2);
    sortedTVLists.add(tvList);

    return new ReadOnlyMemChunkIterator(sortedTVLists, null);
  }

  @Test
  public void testInt64MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(buildLongPointReader());
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.INT64);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.INT64));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private IPointReader buildLongPointReader() {
    List<TVList> sortedTVLists = new ArrayList<>();
    LongTVList tvList = LongTVList.newList();
    tvList.putLong(1L, 1L);
    tvList.putLong(2L, 2L);
    sortedTVLists.add(tvList);

    return new ReadOnlyMemChunkIterator(sortedTVLists, null);
  }

  @Test
  public void testFloatMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(buildFloatPointReader());
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.FLOAT);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.FLOAT));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private IPointReader buildFloatPointReader() {
    List<TVList> sortedTVLists = new ArrayList<>();
    FloatTVList tvList = FloatTVList.newList();
    tvList.putFloat(1L, 1.1f);
    tvList.putFloat(2L, 2.1f);
    sortedTVLists.add(tvList);

    return new ReadOnlyMemChunkIterator(sortedTVLists, null);
  }

  @Test
  public void testDoubleMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(buildDoublePointReader());
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.DOUBLE);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.DOUBLE));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private IPointReader buildDoublePointReader() {
    List<TVList> sortedTVLists = new ArrayList<>();
    DoubleTVList tvList = DoubleTVList.newList();
    tvList.putDouble(1L, 1.1d);
    tvList.putDouble(2L, 2.1d);
    sortedTVLists.add(tvList);

    return new ReadOnlyMemChunkIterator(sortedTVLists, null);
  }

  @Test
  public void testTextMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(buildTextPointReader());
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.TEXT);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.TEXT));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private IPointReader buildTextPointReader() {
    List<TVList> sortedTVLists = new ArrayList<>();
    BinaryTVList tvList = BinaryTVList.newList();
    tvList.putBinary(1L, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
    tvList.putBinary(2L, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
    sortedTVLists.add(tvList);

    return new ReadOnlyMemChunkIterator(sortedTVLists, null);
  }
}
