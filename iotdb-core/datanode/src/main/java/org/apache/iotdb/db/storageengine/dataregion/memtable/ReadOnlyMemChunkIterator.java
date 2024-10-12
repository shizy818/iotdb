package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadOnlyMemChunkIterator implements IPointReader {
  private final List<TVList.TVListIterator> listIterators;
  List<TimeRange> deletionList;
  private boolean hasNextRow;
  private TimeValuePair currentTvPair;

  public ReadOnlyMemChunkIterator(List<TVList> sortedLists, List<TimeRange> deletionList) {
    this.listIterators = new ArrayList<>();
    for (TVList sortedList : sortedLists) {
      listIterators.add(sortedList.iterator());
    }
    this.deletionList = deletionList;
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
            && ModificationUtils.isPointDeleted(currTvPair.getTimestamp(), deletionList)) {
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
        TimeValuePair curr = iterator.current();
        if (curr != null && curr.getTimestamp() == currentTvPair.getTimestamp()) {
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
    TimeValuePair ret = currentTvPair;
    prepareNextRow();
    return ret;
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
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
