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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.datastructure.MergeSortTvListIterator;
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
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.reader.IPointReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.utils.ModificationUtils.isPointDeleted;

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

  protected TsBlock tsBlock;

  private int floatPrecision;
  private TSEncoding encoding;
  private List<TimeRange> deletionList;

  // Read only chunk is now regarded as multiple pages. Apart from chunk statistics,
  // we need to collect page statistic and MergeSortTvListIterator offset for each page.
  private List<Statistics> pageStatisticsList;
  private List<int[]> pageOffsetsList;

  // tvlist rowCount during query
  protected Map<TVList, Integer> tvListQueryMap;

  public static final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  protected ReadOnlyMemChunk(QueryContext context) {
    this.context = context;
  }

  public ReadOnlyMemChunk(
      QueryContext context,
      String measurementUid,
      TSDataType dataType,
      TSEncoding encoding,
      Map<TVList, Integer> tvListQueryMap,
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
    this.floatPrecision = floatPrecision;
    this.encoding = encoding;
    this.deletionList = deletionList;
    this.tvListQueryMap = tvListQueryMap;
    this.pageStatisticsList = new ArrayList<>();
    this.pageOffsetsList = new ArrayList<>();
    this.context.addTVListToSet(tvListQueryMap);

    initChunkAndPageStatistics();
  }

  private void initChunkAndPageStatistics() {
    // create chunk metadata
    Statistics chunkStatistics = Statistics.getStatsByType(dataType);
    IChunkMetadata metaData =
        new ChunkMetadata(measurementUid, dataType, null, null, 0, chunkStatistics);
    metaData.setChunkLoader(new MemChunkLoader(context, this));
    metaData.setVersion(Long.MAX_VALUE);
    cachedMetaData = metaData;

    sortTvLists();
    updateChunkAndPageStatisticsFromTvLists();
  }

  private void sortTvLists() {
    for (Map.Entry<TVList, Integer> entry : getTvListQueryMap().entrySet()) {
      TVList tvList = entry.getKey();
      int queryRowCount = entry.getValue();
      if (!tvList.isSorted() && queryRowCount > tvList.seqRowCount()) {
        tvList.sort();
      }
    }
  }

  private void updateChunkAndPageStatisticsFromTvLists() {
    Statistics chunkStatistics = cachedMetaData.getStatistics();

    int cnt = 0;
    int[] deleteCursor = {0};
    List<TVList> tvLists = new ArrayList<>(tvListQueryMap.keySet());
    System.out.println("tvlist : " + Arrays.toString(tvLists.get(0).getTimestamps().get(0)));
    MergeSortTvListIterator timeValuePairIterator =
        new MergeSortTvListIterator(dataType, encoding, floatPrecision, tvLists);
    int[] tvListOffsets = timeValuePairIterator.getTVListOffsets();
    int[] firstPageStartOffsets = Arrays.copyOf(tvListOffsets, tvListOffsets.length);
    while (timeValuePairIterator.hasNextTimeValuePair()) {
      int[] tvListOffsetsBeforeNext = timeValuePairIterator.getLastTVListOffsets();
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      if (!isPointDeleted(tvPair.getTimestamp(), deletionList, deleteCursor)) {
        if (cnt % MAX_NUMBER_OF_POINTS_IN_PAGE == 0) {
          Statistics stats = Statistics.getStatsByType(dataType);
          pageStatisticsList.add(stats);
          //          pageOffsetsList.add(
          //              Arrays.copyOf(tvListOffsetsBeforeNext, tvListOffsetsBeforeNext.length));
          pageOffsetsList.add(
              cnt == 0
                  ? firstPageStartOffsets
                  : Arrays.copyOf(tvListOffsetsBeforeNext, tvListOffsetsBeforeNext.length));
        }

        Statistics pageStatistics = pageStatisticsList.get(pageStatisticsList.size() - 1);
        switch (dataType) {
          case BOOLEAN:
            chunkStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getBoolean());
            pageStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getBoolean());
            break;
          case INT32:
          case DATE:
            chunkStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getInt());
            pageStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getInt());
            break;
          case INT64:
          case TIMESTAMP:
            chunkStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getLong());
            pageStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getLong());
            break;
          case FLOAT:
            chunkStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getFloat());
            pageStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getFloat());
            break;
          case DOUBLE:
            chunkStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getDouble());
            pageStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getDouble());
            break;
          case TEXT:
          case BLOB:
          case STRING:
            chunkStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getBinary());
            pageStatistics.update(tvPair.getTimestamp(), tvPair.getValue().getBinary());
            break;
          default:
            // do nothing
        }
        pageStatistics.setEmpty(false);
        cnt++;
      }
    }
    chunkStatistics.setEmpty(cnt == 0);
    for (int[] pageOffsets : pageOffsetsList) {
      System.out.println(Arrays.toString(pageOffsets));
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean isEmpty() {
    if (tsBlock == null) {
      return count() == 0;
    }
    return tsBlock.isEmpty();
  }

  public IChunkMetadata getChunkMetaData() {
    return cachedMetaData;
  }

  @TestOnly
  public IPointReader getPointReader() {
    for (Map.Entry<TVList, Integer> entry : tvListQueryMap.entrySet()) {
      TVList tvList = entry.getKey();
      int queryLength = entry.getValue();
      if (!tvList.isSorted() && queryLength > tvList.seqRowCount()) {
        tvList.sort();
      }
    }
    TsBlock tsBlock = buildTsBlock();
    return tsBlock.getTsBlockSingleColumnIterator();
  }

  private TsBlock buildTsBlock() {
    try {
      TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(dataType));
      writeValidValuesIntoTsBlock(builder);
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // read all data in memory chunk and write to tsblock
  private void writeValidValuesIntoTsBlock(TsBlockBuilder builder) throws IOException {
    int[] deleteCursor = {0};
    List<TVList> tvLists = new ArrayList<>(tvListQueryMap.keySet());
    IPointReader timeValuePairIterator =
        new MergeSortTvListIterator(dataType, encoding, floatPrecision, tvLists);

    while (timeValuePairIterator.hasNextTimeValuePair()) {
      TimeValuePair tvPair = timeValuePairIterator.nextTimeValuePair();
      if (!isPointDeleted(tvPair.getTimestamp(), deletionList, deleteCursor)) {
        builder.getTimeColumnBuilder().writeLong(tvPair.getTimestamp());
        switch (dataType) {
          case BOOLEAN:
            builder.getColumnBuilder(0).writeBoolean(tvPair.getValue().getBoolean());
            break;
          case INT32:
          case DATE:
            builder.getColumnBuilder(0).writeInt(tvPair.getValue().getInt());
            break;
          case INT64:
          case TIMESTAMP:
            builder.getColumnBuilder(0).writeLong(tvPair.getValue().getLong());
            break;
          case FLOAT:
            float fv = tvPair.getValue().getFloat();
            if (!Float.isNaN(fv)
                && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
              fv = MathUtils.roundWithGivenPrecision(fv, floatPrecision);
            }
            builder.getColumnBuilder(0).writeFloat(fv);
            break;
          case DOUBLE:
            double dv = tvPair.getValue().getDouble();
            if (!Double.isNaN(dv)
                && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
              dv = MathUtils.roundWithGivenPrecision(dv, floatPrecision);
            }
            builder.getColumnBuilder(0).writeDouble(dv);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            builder.getColumnBuilder(0).writeBinary(tvPair.getValue().getBinary());
            break;
          default:
            break;
        }
        builder.declarePosition();
      }
    }
  }

  public TsBlock getTsBlock() {
    return tsBlock;
  }

  public Map<TVList, Integer> getTvListQueryMap() {
    return tvListQueryMap;
  }

  public int count() {
    int count = 0;
    for (TVList list : tvListQueryMap.keySet()) {
      count += list.count();
    }
    return count;
  }

  public int getFloatPrecision() {
    return floatPrecision;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public List<TimeRange> getDeletionList() {
    return deletionList;
  }

  public List<Statistics> getPageStatisticsList() {
    return pageStatisticsList;
  }

  public List<int[]> getPageOffsetsList() {
    return pageOffsetsList;
  }
}
