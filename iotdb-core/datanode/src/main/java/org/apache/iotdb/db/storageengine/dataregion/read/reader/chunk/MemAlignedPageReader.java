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

import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunkIterator;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchDataFactory;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.series.PaginationController;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;

public class MemAlignedPageReader implements IPageReader {

  private final IPointReader timeValuePairIterator;
  private final AlignedChunkMetadata chunkMetadata;

  private Filter recordFilter;
  private PaginationController paginationController = UNLIMITED_PAGINATION_CONTROLLER;

  private TsBlockBuilder builder;

  public MemAlignedPageReader(
      IPointReader timeValuePairIterator, AlignedChunkMetadata chunkMetadata, Filter recordFilter) {
    this.timeValuePairIterator = timeValuePairIterator;
    this.chunkMetadata = chunkMetadata;
    this.recordFilter = recordFilter;
  }

  @Override
  public BatchData getAllSatisfiedPageData() throws IOException {
    return IPageReader.super.getAllSatisfiedPageData();
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData batchData = BatchDataFactory.createBatchData(TSDataType.VECTOR, ascending, false);
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      if (recordFilter != null
          && !recordFilter.satisfyRow(tvPair.getTimestamp(), tvPair.getValues())) {
        continue;
      }
      batchData.putVector(tvPair.getTimestamp(), tvPair.getValue().getVector());
    }
    ((AlignedReadOnlyMemChunkIterator) timeValuePairIterator).reset();
    return batchData.flip();
  }

  @Override
  public TsBlock getAllSatisfiedData() throws IOException {
    builder.reset();
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();

      // skip unsatisfied row
      if (recordFilter != null
          && !recordFilter.satisfyRow(tvPair.getTimestamp(), tvPair.getValues())) {
        continue;
      }
      // skip offset rows
      if (paginationController.hasCurOffset()) {
        paginationController.consumeOffset();
        continue;
      }

      if (paginationController.hasCurLimit()) {
        // write time column
        writeTimeColumn(builder, tvPair.getTimestamp());
        // write value column
        writeValueColumns(builder, tvPair.getValue().getVector());
        paginationController.consumeLimit();
      } else {
        break;
      }
    }
    ((AlignedReadOnlyMemChunkIterator) timeValuePairIterator).reset();
    return builder.build();
  }

  private void writeTimeColumn(TsBlockBuilder builder, long time) {
    builder.getTimeColumnBuilder().writeLong(time);
    builder.declarePosition();
  }

  private void writeValueColumns(TsBlockBuilder builder, TsPrimitiveType[] values) {
    for (int column = 0; column < values.length; column++) {
      ColumnBuilder valueBuilder = builder.getColumnBuilder(column);
      if (values[column] != null) {
        valueBuilder.writeObject(values[column].getValue());
      } else {
        valueBuilder.appendNull();
      }
    }
  }

  @Override
  public Statistics<? extends Serializable> getStatistics() {
    return chunkMetadata.getStatistics();
  }

  @Override
  public Statistics<? extends Serializable> getTimeStatistics() {
    return chunkMetadata.getTimeStatistics();
  }

  @Override
  public Optional<Statistics<? extends Serializable>> getMeasurementStatistics(
      int measurementIndex) {
    return chunkMetadata.getMeasurementStatistics(measurementIndex);
  }

  @Override
  public boolean hasNullValue(int measurementIndex) {
    return chunkMetadata.hasNullValue(measurementIndex);
  }

  @Override
  public void addRecordFilter(Filter filter) {
    this.recordFilter = FilterFactory.and(recordFilter, filter);
  }

  @Override
  public void setLimitOffset(PaginationController paginationController) {
    this.paginationController = paginationController;
  }

  @Override
  public boolean isModified() {
    return false;
  }

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    builder = new TsBlockBuilder(dataTypes);
  }
}
