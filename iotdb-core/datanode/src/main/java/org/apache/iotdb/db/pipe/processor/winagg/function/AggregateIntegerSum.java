package org.apache.iotdb.db.pipe.processor.winagg.function;

import org.apache.iotdb.db.pipe.processor.winagg.AggregateState;
import org.apache.iotdb.db.pipe.processor.winagg.IncrementalAggregateState;

public class AggregateIntegerSum implements AggregateFunction<Integer, Long, Long> {

  @Override
  public Long createAccumulator() {
    return 0L;
  }

  @Override
  public AggregateState<Integer, Long> createState() {
    return new IncrementalAggregateState<>(this);
  }

  @Override
  public Long add(Integer value, Long accumulator) {
    return value + accumulator;
  }

  @Override
  public Long getResult(Long accumulator) {
    return accumulator;
  }
}
