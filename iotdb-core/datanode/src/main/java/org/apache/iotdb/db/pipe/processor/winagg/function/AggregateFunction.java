package org.apache.iotdb.db.pipe.processor.winagg.function;

import org.apache.iotdb.db.pipe.processor.winagg.AggregateState;

import org.apache.tsfile.enums.TSDataType;

/*
 * @param <IN> The type of the values that are aggregated (input values)
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> The type of the aggregated result
 */
public interface AggregateFunction<IN, ACC, OUT> {
  ACC createAccumulator();

  AggregateState<IN, OUT> createState();

  ACC add(IN value, ACC accumulator);

  OUT getResult(ACC accumulator);

  TSDataType getTsDataType();

  String name();
}
