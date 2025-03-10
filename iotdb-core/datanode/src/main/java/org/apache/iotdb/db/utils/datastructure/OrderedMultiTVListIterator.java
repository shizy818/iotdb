package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;

import java.util.ArrayList;
import java.util.List;

public class OrderedMultiTVListIterator extends MultiTVListIterator {
  public OrderedMultiTVListIterator(
      TSDataType tsDataType,
      List<TVList> tvLists,
      List<TimeRange> deletionList,
      Integer floatPrecision,
      TSEncoding encoding) {
    super(tsDataType, tvLists, deletionList, floatPrecision, encoding);
  }

  private OrderedMultiTVListIterator() {
    super();
  }

  @Override
  public void setTVListOffsets(int[] tvListOffsets) {
    for (int i = 0; i < tvListIterators.size(); i++) {
      tvListIterators.get(i).setIndex(tvListOffsets[i]);
      this.tvListOffsets[i] = tvListOffsets[i];
    }
    probeNext = false;
  }

  @Override
  public MultiTVListIterator clone() {
    OrderedMultiTVListIterator cloneIterator = new OrderedMultiTVListIterator();
    cloneIterator.tsDataType = tsDataType;
    cloneIterator.tvListIterators = new ArrayList<>(tvListIterators.size());
    for (int i = 0; i < tvListIterators.size(); i++) {
      cloneIterator.tvListIterators.add(tvListIterators.get(i).clone());
    }
    cloneIterator.tvListOffsets = new int[tvListIterators.size()];
    return cloneIterator;
  }

  protected void prepareNext() {
    hasNext = false;
    while (!tvListIterators.get(iteratorIndex).hasNext()
        && iteratorIndex < tvListIterators.size() - 1) {
      iteratorIndex++;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    if (iterator.hasNext()) {
      rowIndex = iterator.getIndex();
      hasNext = true;
    }
    probeNext = true;
  }

  protected void next() {
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    iterator.step();
    tvListOffsets[iteratorIndex] = iterator.getIndex();
    probeNext = false;
  }
}
