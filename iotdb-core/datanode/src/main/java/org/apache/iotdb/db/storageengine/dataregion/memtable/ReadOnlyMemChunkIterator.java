package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.db.utils.ModificationUtils;
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

public class ReadOnlyMemChunkIterator implements IPointReader {
  private List<TVList.TVListIterator> listIterators;
  private List<TimeRange> deletionList;
  private boolean hasNextRow;
  private TimeValuePair currentTvPair;
  private TSEncoding encoding;
  private Integer floatPrecision;
  int[] deleteCursor;

  public ReadOnlyMemChunkIterator(List<TVList> sortedLists, List<TimeRange> deletionList) {
    this(sortedLists, deletionList, null, null);
  }

  public ReadOnlyMemChunkIterator(
      List<TVList> sortedLists,
      List<TimeRange> deletionList,
      TSEncoding encoding,
      Integer floatPrecision) {
    this.listIterators = new ArrayList<>();
    for (TVList sortedList : sortedLists) {
      listIterators.add(sortedList.iterator());
    }
    this.deletionList = deletionList;
    this.encoding = encoding;
    this.floatPrecision = floatPrecision;
    this.deleteCursor = new int[] {0};
    prepareNextRow();
  }

  private void prepareNextRow() {
    hasNextRow = false;
    long time = Long.MAX_VALUE;
    int selectedTVListIndex = -1;
    for (int i = 0; i < listIterators.size(); i++) {
      TVList.TVListIterator iterator = listIterators.get(i);
      TimeValuePair currTvPair = iterator.current();
      // skip deleted point
      if (deletionList != null) {
        while (currTvPair != null
            && ModificationUtils.isPointDeleted(
                currTvPair.getTimestamp(), deletionList, deleteCursor)) {
          iterator.next();
          currTvPair = iterator.current();
        }
      }

      // skip duplicated timestamp
      while (currTvPair != null
          && iterator.peekNext() != null
          && iterator.peekNext().getTimestamp() == currTvPair.getTimestamp()) {
        iterator.next();
        currTvPair = iterator.current();
      }

      // update minimum time and remember selected TVList
      if (currTvPair != null && currTvPair.getTimestamp() <= time) {
        time = currTvPair.getTimestamp();
        selectedTVListIndex = i;
      }
    }

    if (selectedTVListIndex >= 0) {
      currentTvPair = listIterators.get(selectedTVListIndex).next();
      hasNextRow = true;

      // call next if current timestamp in other list iterator is identical
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
    TSDataType dataType = currentTvPair.getValue().getDataType();
    if (encoding != null && floatPrecision != null) {
      if (dataType == TSDataType.FLOAT) {
        float value = currentTvPair.getValue().getFloat();
        if (!Float.isNaN(value)
            && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
          currentTvPair.setValue(
              new TsPrimitiveType.TsFloat(
                  MathUtils.roundWithGivenPrecision(value, floatPrecision)));
        }
      } else if (dataType == TSDataType.DOUBLE) {
        double value = currentTvPair.getValue().getDouble();
        if (!Double.isNaN(value)
            && (encoding == TSEncoding.RLE || encoding == TSEncoding.TS_2DIFF)) {
          currentTvPair.setValue(
              new TsPrimitiveType.TsDouble(
                  MathUtils.roundWithGivenPrecision(value, floatPrecision)));
        }
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
    for (TVList.TVListIterator listIterator : listIterators) {
      listIterator.seekToFirst();
    }
    currentTvPair = null;
    prepareNextRow();
  }
}
