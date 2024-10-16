package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlignedReadOnlyMemChunkIterator implements IPointReader {
  private final List<AlignedTVList.AlignedTVListIterator> listIterators;
  private final List<TSDataType> dataTypes;
  private final List<TimeRange> timeColumnDeletion;
  private final List<List<TimeRange>> valueColumnsDeletionList;
  private final int[] deleteCursor;
  private final List<TSEncoding> encodingList;
  private final Integer floatPrecision;
  private boolean hasNextRow;
  private final List<Integer> columnIndexList;
  private final boolean isIgnoreAllNullRows;

  private TimeValuePair currentTvPair;

  public AlignedReadOnlyMemChunkIterator(
      List<AlignedTVList> sortedLists,
      List<TSDataType> dataTypes,
      List<Integer> columnIndexList,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      boolean isIgnoreAllNullRows) {
    this(
        sortedLists,
        dataTypes,
        columnIndexList,
        timeColumnDeletion,
        valueColumnsDeletionList,
        null,
        null,
        isIgnoreAllNullRows);
  }

  public AlignedReadOnlyMemChunkIterator(
      List<AlignedTVList> sortedLists,
      List<TSDataType> dataTypes,
      List<Integer> columnIndexList,
      List<TimeRange> timeColumnDeletion,
      List<List<TimeRange>> valueColumnsDeletionList,
      List<TSEncoding> encodingList,
      Integer floatPrecision,
      boolean isIgnoreAllNullRows) {
    this.listIterators = new ArrayList<>();
    for (AlignedTVList sortedList : sortedLists) {
      listIterators.add(sortedList.iterator(isIgnoreAllNullRows));
    }
    this.dataTypes = dataTypes;
    this.timeColumnDeletion = timeColumnDeletion;
    this.valueColumnsDeletionList = valueColumnsDeletionList;
    this.encodingList = encodingList;
    this.floatPrecision = floatPrecision;
    this.deleteCursor = new int[] {0};
    this.isIgnoreAllNullRows = isIgnoreAllNullRows;
    this.columnIndexList = columnIndexList;
    prepareNextRow();
  }

  private void updateLastValidValueForTimeDup(
      TsPrimitiveType[] lastValidValueForTimeDup, TimeValuePair tvPair) {
    TsPrimitiveType[] values = tvPair.getValue().getVector();
    for (int column = 0; column < values.length; column++) {
      if (values[column] != null) {
        lastValidValueForTimeDup[column] = values[column];
      }
    }
  }

  private boolean isNullRow(TimeValuePair tvPair) {
    TsPrimitiveType[] vector = tvPair.getValue().getVector();
    for (TsPrimitiveType tsPrimitiveType : vector) {
      if (tsPrimitiveType != null) {
        return false;
      }
    }
    return true;
  }

  private boolean needTvPairByColumnIndex(TimeValuePair tvPair) {
    if (columnIndexList == null) {
      return false;
    } else if (columnIndexList.size() != tvPair.getValue().getVector().length) {
      return true;
    }
    for (int i = 0; i < columnIndexList.size(); i++) {
      if (columnIndexList.get(i) != i) {
        return true;
      }
    }
    return false;
  }

  private TimeValuePair handleColumnsInTvPair(TimeValuePair tvPair) {
    if (tvPair == null) {
      return null;
    }
    long time = tvPair.getTimestamp();

    if (needTvPairByColumnIndex(tvPair)) {
      TsPrimitiveType[] oldValues = tvPair.getValue().getVector();
      TsPrimitiveType[] newValues = new TsPrimitiveType[dataTypes.size()];
      for (int column = 0; column < dataTypes.size(); column++) {
        int columnIndex = columnIndexList.get(column);
        // it is possible columnIndex is larger than column size of tvPair
        if (columnIndex >= 0 && columnIndex < oldValues.length) {
          newValues[column] = oldValues[columnIndex];
        }
      }
      tvPair = new TimeValuePair(time, new TsPrimitiveType.TsVector(newValues));
    }

    // deleted columns
    if (valueColumnsDeletionList != null) {
      TsPrimitiveType[] values = tvPair.getValue().getVector();
      for (int column = 0; column < dataTypes.size(); column++) {
        if (values[column] != null
            && ModificationUtils.isPointDeleted(time, valueColumnsDeletionList.get(column))) {
          values[column] = null;
        }
      }
    }

    // skip null row for tree model
    if (isIgnoreAllNullRows && isNullRow(tvPair)) {
      return null;
    }
    return tvPair;
  }

  private void prepareNextRow() {
    hasNextRow = false;
    long time = Long.MAX_VALUE;
    int selectedTVListIndex = -1;
    TsPrimitiveType[] lastValidValueForTimeDup = null;
    for (int i = 0; i < listIterators.size(); i++) {
      AlignedTVList.AlignedTVListIterator iterator = listIterators.get(i);
      TimeValuePair currTvPair = handleColumnsInTvPair(iterator.current());

      // update lastValidValueForTimeDup when same timestamp exists in other TVList
      if (currTvPair != null && currTvPair.getTimestamp() == time) {
        if (lastValidValueForTimeDup == null) {
          lastValidValueForTimeDup = new TsPrimitiveType[dataTypes.size()];
        }
        updateLastValidValueForTimeDup(lastValidValueForTimeDup, currTvPair);
      }

      // skip deleted point
      if (timeColumnDeletion != null) {
        while (currTvPair != null
            && ModificationUtils.isPointDeleted(
                currTvPair.getTimestamp(), timeColumnDeletion, deleteCursor)) {
          iterator.next();
          currTvPair = handleColumnsInTvPair(iterator.current());
        }
      }

      // duplicated timestamp in current TVList
      while (currTvPair != null
          && iterator.peekNext() != null
          && iterator.peekNext().getTimestamp() == currTvPair.getTimestamp()) {
        iterator.next();
        currTvPair = handleColumnsInTvPair(iterator.current());
        // update lastValidValueForTimeDup
        if (lastValidValueForTimeDup == null) {
          lastValidValueForTimeDup = new TsPrimitiveType[dataTypes.size()];
        }
        updateLastValidValueForTimeDup(lastValidValueForTimeDup, currTvPair);
      }

      // update minimum time and remember selected TVList
      if (currTvPair != null && currTvPair.getTimestamp() <= time) {
        if (currTvPair.getTimestamp() < time) {
          time = currTvPair.getTimestamp();
          lastValidValueForTimeDup = null;
        }
        selectedTVListIndex = i;
      }
    }

    if (selectedTVListIndex >= 0) {
      currentTvPair = handleColumnsInTvPair(listIterators.get(selectedTVListIndex).next());
      // update not null value from lastValidValueForTimeDup
      if (lastValidValueForTimeDup != null) {
        TsPrimitiveType[] values = currentTvPair.getValue().getVector();
        for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
          if (lastValidValueForTimeDup[columnIndex] != null) {
            values[columnIndex] = lastValidValueForTimeDup[columnIndex];
          }
        }
      }
      hasNextRow = true;

      // call next if current timestamp in other list is identical
      for (TVList.TVListIterator iterator : listIterators) {
        TimeValuePair tvPair = iterator.current();
        if (tvPair != null && tvPair.getTimestamp() == currentTvPair.getTimestamp()) {
          iterator.next();
        }
      }
    }
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    return hasNextRow;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    TimeValuePair ret = currentTimeValuePair();
    prepareNextRow();
    return ret;
  }

  @Override
  public TimeValuePair currentTimeValuePair() {
    if (encodingList == null || floatPrecision == null) {
      return currentTvPair;
    }

    TsPrimitiveType[] values = currentTvPair.getValue().getVector();
    for (int i = 0; i < values.length; i++) {
      TsPrimitiveType value = values[i];
      if (value == null) {
        continue;
      }
      TSDataType dataType = value.getDataType();
      if (dataType == TSDataType.FLOAT) {
        if (!Float.isNaN(value.getFloat())
            && (encodingList.get(i) == TSEncoding.RLE
                || encodingList.get(i) == TSEncoding.TS_2DIFF)) {
          values[i] =
              new TsPrimitiveType.TsFloat(
                  MathUtils.roundWithGivenPrecision(value.getFloat(), floatPrecision));
        }
      } else if (dataType == TSDataType.DOUBLE) {
        if (!Double.isNaN(value.getDouble())
            && (encodingList.get(i) == TSEncoding.RLE
                || encodingList.get(i) == TSEncoding.TS_2DIFF))
          values[i] =
              new TsPrimitiveType.TsDouble(
                  MathUtils.roundWithGivenPrecision(value.getDouble(), floatPrecision));
      }
    }
    return currentTvPair;
  }

  @Override
  public long getUsedMemorySize() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    listIterators.clear();
  }

  public void reset() {
    for (AlignedTVList.AlignedTVListIterator listIterator : listIterators) {
      listIterator.seekToFirst();
    }
    currentTvPair = null;
    prepareNextRow();
  }
}
