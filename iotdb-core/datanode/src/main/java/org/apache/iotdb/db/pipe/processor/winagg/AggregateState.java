package org.apache.iotdb.db.pipe.processor.winagg;

import org.apache.tsfile.enums.TSDataType;

public interface AggregateState<IN, OUT> {
  OUT get();

  void add(IN value);

  TSDataType getTsDataType();

  String name();
}
