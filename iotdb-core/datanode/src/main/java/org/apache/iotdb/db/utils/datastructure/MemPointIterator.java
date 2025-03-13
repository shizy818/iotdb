package org.apache.iotdb.db.utils.datastructure;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPointReader;

public interface MemPointIterator extends IPointReader {
  TsBlock getBatch(int tsBlockIndex);

  boolean hasNextBatch();

  TsBlock nextBatch();

  MemPointIterator clone();
}
