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
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * ReadOnlyMemChunk is a snapshot of the working MemTable and flushing memtable in the memory used
 * for querying.
 */
public class ReadOnlyMemChunk {

  protected final QueryContext context;

  private String measurementUid;

  private TSDataType dataType;

  private static final Logger logger = LoggerFactory.getLogger(ReadOnlyMemChunk.class);

  protected IChunkMetadata cachedMetaData;

  private List<TVList> sortedLists;
  List<TimeRange> deletionList;

  protected ReadOnlyMemChunk(QueryContext context) {
    this.context = context;
  }

  public ReadOnlyMemChunk(
      QueryContext context,
      String measurementUid,
      TSDataType dataType,
      TSEncoding encoding,
      List<TVList> sortedLists,
      Map<String, String> props,
      List<TimeRange> deletionList)
      throws IOException, QueryProcessException {
    this.context = context;
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    if (props != null && props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      try {
        floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
      } catch (NumberFormatException e) {
        logger.warn(
            "The format of MAX_POINT_NUMBER {}  is not correct."
                + " Using default float precision.",
            props.get(Encoder.MAX_POINT_NUMBER));
      }
      if (floatPrecision < 0) {
        logger.warn(
            "The MAX_POINT_NUMBER shouldn't be less than 0." + " Using default float precision {}.",
            TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
        floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      }
    }
    this.sortedLists = sortedLists;
    this.deletionList = deletionList;
    initChunkMetaFromTvLists();
  }

  private void initChunkMetaFromTvLists() throws QueryProcessException, IOException {
    Statistics<? extends Serializable> statsByType = Statistics.getStatsByType(dataType);
    IChunkMetadata metaData =
        new ChunkMetadata(measurementUid, dataType, null, null, 0, statsByType);

    ReadOnlyMemChunkIterator iterator = new ReadOnlyMemChunkIterator(sortedLists, deletionList);
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = iterator.nextTimeValuePair();
      switch (dataType) {
        case BOOLEAN:
          statsByType.update(tvPair.getTimestamp(), tvPair.getValue().getBoolean());
          break;
        case INT32:
        case DATE:
          statsByType.update(tvPair.getTimestamp(), tvPair.getValue().getInt());
          break;
        case INT64:
        case TIMESTAMP:
          statsByType.update(tvPair.getTimestamp(), tvPair.getValue().getLong());
          break;
        case TEXT:
        case BLOB:
        case STRING:
          statsByType.update(tvPair.getTimestamp(), tvPair.getValue().getBinary());
          break;
        case FLOAT:
          statsByType.update(tvPair.getTimestamp(), tvPair.getValue().getFloat());
          break;
        case DOUBLE:
          statsByType.update(tvPair.getTimestamp(), tvPair.getValue().getDouble());
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    }
    statsByType.setEmpty(isEmpty());
    metaData.setChunkLoader(new MemChunkLoader(context, this));
    metaData.setVersion(Long.MAX_VALUE);
    cachedMetaData = metaData;
  }

  public long rowCount() {
    long rowCount = 0;
    for (TVList sortedList : sortedLists) {
      rowCount += sortedList.rowCount();
    }
    return rowCount;
  }

  public boolean isEmpty() {
    return rowCount() == 0;
  }

  public IChunkMetadata getChunkMetaData() {
    return cachedMetaData;
  }

  public IPointReader getPointReader() {
    return new ReadOnlyMemChunkIterator(sortedLists, deletionList);
  }
}
