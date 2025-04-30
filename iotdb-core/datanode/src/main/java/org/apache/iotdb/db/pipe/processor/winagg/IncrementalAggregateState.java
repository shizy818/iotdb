package org.apache.iotdb.db.pipe.processor.winagg;

import org.apache.iotdb.db.pipe.processor.winagg.function.AggregateFunction;

public class IncrementalAggregateState<IN, ACC, OUT> implements AggregateState<IN, OUT> {
  private final AggregateFunction<IN, ACC, OUT> aggregateFunction;
  private ACC accumulator;

  public IncrementalAggregateState(AggregateFunction<IN, ACC, OUT> aggregateFunction) {
    this.aggregateFunction = aggregateFunction;
    this.accumulator = aggregateFunction.createAccumulator();
  }

  @Override
  public OUT get() {
    return accumulator != null ? aggregateFunction.getResult(accumulator) : null;
  }

  @Override
  public void add(IN value) {
    accumulator = aggregateFunction.add(value, accumulator);
  }
}
