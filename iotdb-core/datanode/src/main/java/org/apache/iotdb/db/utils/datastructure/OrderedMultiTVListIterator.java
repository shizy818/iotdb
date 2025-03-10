package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;

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
  public MultiTVListIterator clone() {
    OrderedMultiTVListIterator cloneIterator = new OrderedMultiTVListIterator();
    cloneIterator.tsDataType = tsDataType;
    cloneIterator.tsBlocks = tsBlocks;
    return cloneIterator;
  }

  @Override
  protected void prepareNext() {
    hasNext = false;
    while (iteratorIndex < tvListIterators.size() - 1
        && !tvListIterators.get(iteratorIndex).hasNext()) {
      iteratorIndex++;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    if (iterator.hasNext()) {
      rowIndex = iterator.getIndex();
      hasNext = true;
    }
    probeNext = true;
  }

  @Override
  protected void next() {
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    iterator.step();
    probeNext = false;
  }
}
