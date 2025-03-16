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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

public class MultiTVListIteratorFactory {
  private MultiTVListIteratorFactory() {
    // forbidden construction
  }

  // MergeSortMultiTVListIterator
  public static MultiTVListIterator mergeSort(List<TVList> tvLists) {
    return new MergeSortMultiTVListIterator(tvLists, null, null);
  }

  public static MultiTVListIterator mergeSort(
      List<TVList> tvLists, Integer floatPrecision, TSEncoding encoding) {
    return new MergeSortMultiTVListIterator(tvLists, floatPrecision, encoding);
  }

  // OrderedMultiTVListIterator
  public static MultiTVListIterator ordered(List<TVList> tvLists) {
    return new OrderedMultiTVListIterator(tvLists, null, null);
  }

  public static MultiTVListIterator ordered(
      List<TVList> tvLists, Integer floatPrecision, TSEncoding encoding) {
    return new OrderedMultiTVListIterator(tvLists, floatPrecision, encoding);
  }

  // MergeSortMultiAlignedTVListIterator
  public static MultiAlignedTVListIterator mergeSort(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      boolean ignoreAllNullRows) {
    return new MergeSortMultiAlignedTVListIterator(
        alignedTvLists, tsDataTypes, columnIndexList, null, null, ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator mergeSort(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    return new MergeSortMultiAlignedTVListIterator(
        alignedTvLists,
        tsDataTypes,
        columnIndexList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows);
  }

  // OrderedMultiAlignedTVListIterator
  public static MultiAlignedTVListIterator ordered(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      boolean ignoreAllNullRows) {
    return new OrderedMultiAlignedTVListIterator(
        alignedTvLists, tsDataTypes, columnIndexList, null, null, ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator ordered(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    return new OrderedMultiAlignedTVListIterator(
        alignedTvLists,
        tsDataTypes,
        columnIndexList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows);
  }

  public static MultiTVListIterator create(List<TVList> tvLists) {
    if (isCompleteOrdered(tvLists)) {
      return ordered(tvLists);
    } else {
      return mergeSort(tvLists);
    }
  }

  public static MultiTVListIterator create(
      List<TVList> tvLists, Integer floatPrecision, TSEncoding encoding) {
    if (isCompleteOrdered(tvLists)) {
      return ordered(tvLists, floatPrecision, encoding);
    } else {
      return mergeSort(tvLists, floatPrecision, encoding);
    }
  }

  public static MultiAlignedTVListIterator create(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      boolean ignoreAllNullRows) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(alignedTvLists, tsDataTypes, columnIndexList, ignoreAllNullRows);
    } else {
      return mergeSort(alignedTvLists, tsDataTypes, columnIndexList, ignoreAllNullRows);
    }
  }

  public static MultiAlignedTVListIterator create(
      List<AlignedTVList> alignedTvLists,
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(
          alignedTvLists,
          tsDataTypes,
          columnIndexList,
          floatPrecision,
          encodingList,
          ignoreAllNullRows);
    } else {
      return mergeSort(
          alignedTvLists,
          tsDataTypes,
          columnIndexList,
          floatPrecision,
          encodingList,
          ignoreAllNullRows);
    }
  }

  private static boolean isCompleteOrdered(List<? extends TVList> tvLists) {
    long time = Long.MIN_VALUE;
    for (TVList list : tvLists) {
      if (list.rowCount() == 0) {
        continue;
      }
      if (!list.isSorted() || list.getTime(0) <= time) {
        return false;
      }
      time = list.getTime(list.rowCount() - 1);
    }
    return true;
  }
}
