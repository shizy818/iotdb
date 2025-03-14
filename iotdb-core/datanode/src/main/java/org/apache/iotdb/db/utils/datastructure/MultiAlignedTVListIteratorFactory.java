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
import org.apache.tsfile.read.common.TimeRange;

import java.util.List;

public class MultiAlignedTVListIteratorFactory {
  private MultiAlignedTVListIteratorFactory() {
    // forbidden construction
  }

  public static MultiAlignedTVListIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      boolean ignoreAllNullRows) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, null, null, null, null, ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        timeColumnDeletion,
        valueColumnsDeletionList,
        null,
        null,
        ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator mergeSort(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    return new MergeSortMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        timeColumnDeletion,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      boolean ignoreAllNullRows) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes, columnIndexList, alignedTvLists, null, null, null, null, ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        timeColumnDeletion,
        valueColumnsDeletionList,
        null,
        null,
        ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator ordered(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    return new OrderedMultiAlignedTVListIterator(
        tsDataTypes,
        columnIndexList,
        alignedTvLists,
        timeColumnDeletion,
        valueColumnsDeletionList,
        floatPrecision,
        encodingList,
        ignoreAllNullRows);
  }

  public static MultiAlignedTVListIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      boolean ignoreAllNullRows) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(tsDataTypes, columnIndexList, alignedTvLists, ignoreAllNullRows);
    } else {
      return mergeSort(tsDataTypes, columnIndexList, alignedTvLists, ignoreAllNullRows);
    }
  }

  public static MultiAlignedTVListIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean ignoreAllNullRows) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          timeColumnDeletion,
          valueColumnsDeletionList,
          ignoreAllNullRows);
    } else {
      return mergeSort(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          timeColumnDeletion,
          valueColumnsDeletionList,
          ignoreAllNullRows);
    }
  }

  public static MultiAlignedTVListIterator create(
      List<TSDataType> tsDataTypes,
      List<Integer> columnIndexList,
      List<AlignedTVList> alignedTvLists,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      Integer floatPrecision,
      List<TSEncoding> encodingList,
      boolean ignoreAllNullRows) {
    if (isCompleteOrdered(alignedTvLists)) {
      return ordered(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          timeColumnDeletion,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          ignoreAllNullRows);
    } else {
      return mergeSort(
          tsDataTypes,
          columnIndexList,
          alignedTvLists,
          timeColumnDeletion,
          valueColumnsDeletionList,
          floatPrecision,
          encodingList,
          ignoreAllNullRows);
    }
  }

  private static boolean isCompleteOrdered(List<AlignedTVList> alignedTvLists) {
    long time = Long.MIN_VALUE;
    for (AlignedTVList list : alignedTvLists) {
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
