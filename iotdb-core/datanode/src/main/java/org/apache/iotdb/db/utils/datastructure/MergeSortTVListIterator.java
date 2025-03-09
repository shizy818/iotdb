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

import org.apache.iotdb.db.utils.MathUtils;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeSortTVListIterator implements IPointReader {
  private TSDataType tsDataType;
  private List<TVList.TVListIterator> tvListIterators;
  private int[] tvListOffsets;
  private Integer floatPrecision;
  private TSEncoding encoding;
  private List<TimeRange> deletionList;

  private boolean probeNext = false;
  private boolean hasNext = false;
  private int iteratorIndex = 0;
  private int rowIndex = 0;

  private List<Integer> probeIterators;
  private final PriorityQueue<Pair<Long, Integer>> minHeap =
      new PriorityQueue<>(
          (a, b) -> a.left.equals(b.left) ? b.right.compareTo(a.right) : a.left.compareTo(b.left));

  protected final int MAX_NUMBER_OF_POINTS_IN_PAGE =
      TSFileDescriptor.getInstance().getConfig().getMaxNumberOfPointsInPage();

  public MergeSortTVListIterator(
      TSDataType tsDataType, List<TVList> tvLists, List<TimeRange> deletionList) {
    this.tsDataType = tsDataType;
    tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(deletionList));
    }
    this.deletionList = deletionList;
    this.tvListOffsets = new int[tvLists.size()];
    this.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
  }

  public MergeSortTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    this.tsDataType = tsDataType;
    this.tvListIterators = new ArrayList<>(tvLists.size());
    for (TVList tvList : tvLists) {
      tvListIterators.add(tvList.iterator(deletionList));
    }
    this.deletionList = deletionList;
    this.floatPrecision = floatPrecision;
    this.encoding = encoding;
    this.tvListOffsets = new int[tvLists.size()];
    this.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
  }

  private MergeSortTVListIterator() {}

  private void prepareNext() {
    hasNext = false;
    if (tvListIterators.size() == 1) {
      iteratorIndex = 0;
      TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
      if (iterator.hasNext()) {
        rowIndex = iterator.getIndex();
        hasNext = true;
      }
      probeNext = true;
      return;
    }

    for (int i : probeIterators) {
      TVList.TVListIterator iterator = tvListIterators.get(i);
      if (iterator.hasNext()) {
        minHeap.add(new Pair<>(iterator.currentTime(), i));
      }
    }
    probeIterators.clear();

    if (!minHeap.isEmpty()) {
      Pair<Long, Integer> top = minHeap.poll();
      iteratorIndex = top.right;
      probeIterators.add(iteratorIndex);
      rowIndex = tvListIterators.get(iteratorIndex).getIndex();
      hasNext = true;
      while (!minHeap.isEmpty() && minHeap.peek().left.longValue() == top.left.longValue()) {
        Pair<Long, Integer> element = minHeap.poll();
        probeIterators.add(element.right);
      }
    }
    probeNext = true;
  }

  @Override
  public boolean hasNextTimeValuePair() {
    if (!probeNext) {
      prepareNext();
    }
    return hasNext;
  }

  @Override
  public TimeValuePair nextTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    TimeValuePair currentTvPair =
        iterator
            .getTVList()
            .getTimeValuePair(rowIndex, iterator.currentTime(), floatPrecision, encoding);
    next();
    return currentTvPair;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (!hasNextTimeValuePair()) {
      return null;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    return iterator
        .getTVList()
        .getTimeValuePair(rowIndex, iterator.currentTime(), floatPrecision, encoding);
  }

  public void next() {
    if (tvListIterators.size() == 1) {
      TVList.TVListIterator iterator = tvListIterators.get(0);
      iterator.step();
      tvListOffsets[0] = iterator.getIndex();
    } else {
      for (int index : probeIterators) {
        TVList.TVListIterator iterator = tvListIterators.get(index);
        iterator.step();
        tvListOffsets[index] = iterator.getIndex();
      }
    }
    probeNext = false;
  }

  public boolean hasNextBatch() {
    return hasNextTimeValuePair();
  }

  public TsBlock nextBatch() {
    return nextBatch(null);
  }

  public TsBlock nextBatch(int[] pageEndOffsets) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(tsDataType));
    int pointsInBatch = 0;
    while (hasNextTimeValuePair() && pointsInBatch < MAX_NUMBER_OF_POINTS_IN_PAGE) {
      if (isOutOfMemPageBounds(pageEndOffsets)) {
        break;
      }

      TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
      builder.getTimeColumnBuilder().writeLong(iterator.currentTime());
      switch (tsDataType) {
        case BOOLEAN:
          builder.getColumnBuilder(0).writeBoolean(iterator.getTVList().getBoolean(rowIndex));
          break;
        case INT32:
        case DATE:
          builder.getColumnBuilder(0).writeInt(iterator.getTVList().getInt(rowIndex));
          break;
        case INT64:
        case TIMESTAMP:
          builder.getColumnBuilder(0).writeLong(iterator.getTVList().getLong(rowIndex));
          break;
        case FLOAT:
          float fValue = iterator.getTVList().getFloat(rowIndex);
          if (!Float.isNaN(fValue)
              && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
            fValue = MathUtils.roundWithGivenPrecision(fValue, floatPrecision);
          }
          builder.getColumnBuilder(0).writeFloat(fValue);
          break;
        case DOUBLE:
          double dValue = iterator.getTVList().getDouble(rowIndex);
          if (!Double.isNaN(dValue)
              && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
            dValue = MathUtils.roundWithGivenPrecision(dValue, floatPrecision);
          }
          builder.getColumnBuilder(0).writeDouble(dValue);
          break;
        case TEXT:
        case BLOB:
        case STRING:
          builder.getColumnBuilder(0).writeBinary(iterator.getTVList().getBinary(rowIndex));
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", tsDataType));
      }
      next();

      builder.declarePosition();
      pointsInBatch++;
    }
    probeNext = false;
    return builder.build();
  }

  @Override
  public long getUsedMemorySize() {
    // not used
    return 0;
  }

  @Override
  public void close() throws IOException {}

  public int[] getTVListOffsets() {
    return tvListOffsets;
  }

  public void setTVListOffsets(int[] tvListOffsets) {
    for (int i = 0; i < tvListIterators.size(); i++) {
      tvListIterators.get(i).setIndex(tvListOffsets[i]);
      this.tvListOffsets[i] = tvListOffsets[i];
    }
    if (tvListIterators.size() > 1) {
      minHeap.clear();
      probeIterators.clear();
      for (int i = 0; i < tvListIterators.size(); i++) {
        probeIterators.add(i);
      }
    }
    probeNext = false;
  }

  @Override
  public MergeSortTVListIterator clone() {
    MergeSortTVListIterator cloneIterator = new MergeSortTVListIterator();
    cloneIterator.tsDataType = tsDataType;
    cloneIterator.tvListIterators = new ArrayList<>(tvListIterators.size());
    for (int i = 0; i < tvListIterators.size(); i++) {
      cloneIterator.tvListIterators.add(tvListIterators.get(i).clone());
    }
    cloneIterator.tvListOffsets = new int[tvListIterators.size()];
    cloneIterator.probeIterators =
        IntStream.range(0, tvListIterators.size()).boxed().collect(Collectors.toList());
    return cloneIterator;
  }

  private boolean isOutOfMemPageBounds(int[] pageEndOffsets) {
    if (pageEndOffsets == null) {
      return false;
    }
    for (int i = 0; i < pageEndOffsets.length; i++) {
      if (tvListOffsets[i] < pageEndOffsets[i]) {
        return false;
      }
    }
    return true;
  }
}
