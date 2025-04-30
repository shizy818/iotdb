package org.apache.iotdb.db.pipe.processor.winagg;

public interface AggregateState<IN, OUT> {
  OUT get();

  void add(IN value);
}
