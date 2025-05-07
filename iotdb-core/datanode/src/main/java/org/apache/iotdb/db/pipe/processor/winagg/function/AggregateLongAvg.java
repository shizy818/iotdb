package org.apache.iotdb.db.pipe.processor.winagg.function;

import org.apache.iotdb.db.pipe.processor.winagg.AggregateState;
import org.apache.iotdb.db.pipe.processor.winagg.IncrementalAggregateState;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tsfile.enums.TSDataType;

public class AggregateLongAvg implements AggregateFunction<Long, Pair<Long, Integer>, Double> {
  @Override
  public Pair<Long, Integer> createAccumulator() {
    return Pair.of(0L, 0);
  }

  @Override
  public AggregateState<Long, Double> createState() {
    return new IncrementalAggregateState<>(this);
  }

  @Override
  public Pair<Long, Integer> add(Long value, Pair<Long, Integer> accumulator) {
    return Pair.of(accumulator.getLeft() + value, accumulator.getRight() + 1);
  }

  @Override
  public Double getResult(Pair<Long, Integer> accumulator) {
    return (double) accumulator.getLeft() / accumulator.getRight();
  }

  @Override
  public TSDataType getTsDataType() {
    return TSDataType.DOUBLE;
  }

  public String name() {
    return "avg";
  }
}
