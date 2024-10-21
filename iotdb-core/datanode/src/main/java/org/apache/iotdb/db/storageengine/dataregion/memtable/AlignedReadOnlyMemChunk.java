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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemAlignedChunkLoader;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlignedReadOnlyMemChunk extends ReadOnlyMemChunk {
  private final String timeChunkName;

  private final List<String> valueChunkNames;

  private final List<TSDataType> dataTypes;

  private final List<Integer> columnIndexList;

  private final List<AlignedTVList> sortedLists;
  private final List<TimeRange> timeColumnDeletion;
  private final List<List<TimeRange>> valueColumnsDeletionList;
  private final List<TSEncoding> encodingList;
  private final int floatPrecision;

  /**
   * The constructor for Aligned type.
   *
   * @param schema VectorMeasurementSchema
   * @param tvList VectorTvList
   * @param deletionList The timeRange of deletionList
   * @throws QueryProcessException if there is unsupported data type.
   */
  public AlignedReadOnlyMemChunk(
      QueryContext context,
      IMeasurementSchema schema,
      List<Integer> columnIndexList,
      List<AlignedTVList> sortedLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList)
      throws QueryProcessException, IOException {
    super(context);
    this.timeChunkName = schema.getMeasurementId();
    this.valueChunkNames = schema.getSubMeasurementsList();
    this.dataTypes = schema.getSubMeasurementsTSDataTypeList();
    this.floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    this.encodingList = schema.getSubMeasurementsTSEncodingList();
    this.sortedLists = sortedLists;
    this.timeColumnDeletion = timeColumnDeletion;
    this.valueColumnsDeletionList = valueColumnsDeletionList;
    this.columnIndexList = columnIndexList;
    initAlignedChunkMetaFromAlignedTvList();
  }

  private void initAlignedChunkMetaFromAlignedTvList() throws IOException, QueryProcessException {
    // Time statistics
    Statistics timeStatistics = Statistics.getStatsByType(TSDataType.VECTOR);
    IChunkMetadata timeChunkMetadata =
        new ChunkMetadata(timeChunkName, TSDataType.VECTOR, null, null, 0, timeStatistics);
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    // values statistics
    Statistics[] valuesStatistics = new Statistics[dataTypes.size()];
    for (int column = 0; column < dataTypes.size(); column++) {
      Statistics valueStatistics = Statistics.getStatsByType(dataTypes.get(column));
      valueStatistics.setEmpty(true);
      valuesStatistics[column] = valueStatistics;
    }

    AlignedReadOnlyMemChunkIterator iterator =
        new AlignedReadOnlyMemChunkIterator(
            sortedLists,
            dataTypes,
            columnIndexList,
            timeColumnDeletion,
            valueColumnsDeletionList,
            context.isIgnoreAllNullRows());

    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = iterator.nextTimeValuePair();
      long time = tvPair.getTimestamp();
      TsPrimitiveType[] vector = tvPair.getValue().getVector();
      // Update time chunk
      timeStatistics.update(time);
      // Update value chunk
      for (int column = 0; column < dataTypes.size(); column++) {
        if (vector[column] == null) {
          continue;
        }
        Statistics valueStatistics = valuesStatistics[column];
        switch (dataTypes.get(column)) {
          case BOOLEAN:
            valueStatistics.update(time, vector[column].getBoolean());
            break;
          case TEXT:
          case BLOB:
          case STRING:
            valueStatistics.update(time, vector[column].getBinary());
            break;
          case FLOAT:
            valueStatistics.update(time, vector[column].getFloat());
            break;
          case INT32:
          case DATE:
            valueStatistics.update(time, vector[column].getInt());
            break;
          case INT64:
          case TIMESTAMP:
            valueStatistics.update(time, vector[column].getLong());
            break;
          case DOUBLE:
            valueStatistics.update(time, vector[column].getDouble());
            break;
          default:
            throw new QueryProcessException("Unsupported data type:" + dataTypes.get(column));
        }
      }
    }
    timeStatistics.setEmpty(false);
    for (int column = 0; column < dataTypes.size(); column++) {
      Statistics valueStatistics = valuesStatistics[column];
      if (valueStatistics.getCount() > 0) {
        IChunkMetadata valueChunkMetadata =
            new ChunkMetadata(
                valueChunkNames.get(column), dataTypes.get(column), null, null, 0, valueStatistics);
        valueChunkMetadataList.add(valueChunkMetadata);
        valueStatistics.setEmpty(false);
      } else {
        valueChunkMetadataList.add(null);
      }
    }
    IChunkMetadata alignedChunkMetadata =
        new AlignedChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
    alignedChunkMetadata.setChunkLoader(new MemAlignedChunkLoader(context, this));
    alignedChunkMetadata.setVersion(Long.MAX_VALUE);
    cachedMetaData = alignedChunkMetadata;
  }

  public int rowCount() {
    int rowCount = 0;
    for (AlignedTVList sortedList : sortedLists) {
      rowCount += sortedList.validRowCount();
    }
    return rowCount;
  }

  @Override
  public boolean isEmpty() {
    return rowCount() == 0;
  }

  @Override
  public IPointReader getPointReader() {
    return new AlignedReadOnlyMemChunkIterator(
        sortedLists,
        dataTypes,
        columnIndexList,
        timeColumnDeletion,
        valueColumnsDeletionList,
        encodingList,
        floatPrecision,
        context.isIgnoreAllNullRows());
  }
}
