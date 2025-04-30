package org.apache.iotdb.db.pipe.processor.winagg.function;

import org.apache.iotdb.db.pipe.processor.winagg.AggregateState;
import org.apache.iotdb.db.pipe.processor.winagg.IncrementalAggregateState;

import org.apache.commons.lang3.tuple.Pair;

public class AggregateIntegerAvg
    implements AggregateFunction<Integer, Pair<Long, Integer>, Double> {
  @Override
  public Pair<Long, Integer> createAccumulator() {
    return Pair.of(0L, 0);
  }

  @Override
  public AggregateState<Integer, Double> createState() {
    return new IncrementalAggregateState<>(this);
  }

  @Override
  public Pair<Long, Integer> add(Integer value, Pair<Long, Integer> accumulator) {
    return Pair.of(accumulator.getLeft() + value, accumulator.getRight() + 1);
  }

  @Override
  public Double getResult(Pair<Long, Integer> accumulator) {
    return (double) accumulator.getLeft() / accumulator.getRight();
  }
}
