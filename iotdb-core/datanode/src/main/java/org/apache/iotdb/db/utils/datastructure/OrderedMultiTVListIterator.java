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
  public MemPointIterator clone() {
    OrderedMultiTVListIterator cloneIterator = new OrderedMultiTVListIterator();
    cloneIterator.tsDataType = tsDataType;
    cloneIterator.tsBlocks.addAll(tsBlocks);
    for (TVList.TVListIterator iterator : tvListIterators) {
      cloneIterator.tvListIterators.add(iterator.clone());
    }
    cloneIterator.floatPrecision = floatPrecision;
    cloneIterator.encoding = encoding;
    return cloneIterator;
  }

  @Override
  protected void prepareNext() {
    hasNext = false;
    while (iteratorIndex < tvListIterators.size() - 1
        && !tvListIterators.get(iteratorIndex).hasNextTimeValuePair()) {
      iteratorIndex++;
    }
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    if (iterator.hasNextTimeValuePair()) {
      rowIndex = iterator.getIndex();
      hasNext = true;
    }
    probeNext = true;
  }

  @Override
  protected void next() {
    TVList.TVListIterator iterator = tvListIterators.get(iteratorIndex);
    iterator.next();
    probeNext = false;
  }
}
