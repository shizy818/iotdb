/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.processor.reactive;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.db.pipe.event.common.row.PipeResetTabletRow;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.access.Row;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.collector.RowCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_EXPR;

@TreeModel
@TableModel
public class ExpressionProcessor implements PipeProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionProcessor.class);

  private static final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
  private static final SqlParser RELATIONAL_SQL_PARSER = new SqlParser();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private IClientSession clientSession;

  private static final Context graalVMConext = Context.create("js");
  private String expr;

  private String outputTimeSeries;

  private MeasurementSchema[] measurementSchemaList;
  private String[] columnNameStringList;
  private TSDataType[] valueColumnTypes;
  private Object[] valueColumns;
  private BitMap[] bitMaps;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PROCESSOR_EXPR);

    final PipeParameters parameters = validator.getParameters();
    expr = parameters.getString(PROCESSOR_EXPR).toLowerCase();
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    final PipeRuntimeEnvironment environment = configuration.getRuntimeEnvironment();
    String dataBaseName =
        StorageEngine.getInstance()
            .getDataRegion(new DataRegionId(environment.getRegionId()))
            .getDatabaseName();

    outputTimeSeries = dataBaseName + TsFileConstant.PATH_SEPARATOR + "t2";
    columnNameStringList = new String[] {"c0", "c1"};
    measurementSchemaList =
        new MeasurementSchema[] {
          new MeasurementSchema("c0", TSDataType.FLOAT),
          new MeasurementSchema("c1", TSDataType.INT32)
        };
    valueColumnTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.INT32};

    //    measurementSchemaList =
    //            new MeasurementSchema[columnNameStringList.length];
    //    valueColumnTypes = new TSDataType[columnNameStringList.length];

    valueColumns = new Object[columnNameStringList.length];
    bitMaps = new BitMap[columnNameStringList.length];
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      eventCollector.collect(tabletInsertionEvent);
      return;
    }

    final Iterable<TabletInsertionEvent> outputEvents =
        tabletInsertionEvent.processRowByRow(this::processRow);

    outputEvents.forEach(
        event -> {
          try {
            eventCollector.collect(event);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws IOException {
    eventCollector.collect(tsFileInsertionEvent);
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    if (clientSession != null) {
      SESSION_MANAGER.closeSession(clientSession, COORDINATOR::cleanupQueryExecution);
    }
  }

  private void processRow(final Row row, final RowCollector rowCollector) {
    // calculate
    int value = row.getInt(1);
    Value bindings = graalVMConext.getBindings("js");
    bindings.putMember("c1", value);
    Value result = graalVMConext.eval("js", expr);

    // time
    final long[] timestampColumn = new long[] {row.getTime()};
    // value
    for (int columnIndex = 0; columnIndex < columnNameStringList.length; ++columnIndex) {
      switch (valueColumnTypes[columnIndex]) {
        case INT32:
          valueColumns[columnIndex] = new int[1];
          ((int[]) valueColumns[columnIndex])[0] =
              columnIndex == 1 ? result.asInt() : row.getInt(columnIndex);
          break;
        case FLOAT:
          valueColumns[columnIndex] = new float[1];
          ((float[]) valueColumns[columnIndex])[0] = row.getFloat(columnIndex);
          break;
        default:
          break;
      }
      bitMaps[columnIndex] = new BitMap(1);
    }

    try {
      rowCollector.collectRow(
          new PipeResetTabletRow(
              0,
              outputTimeSeries,
              true,
              measurementSchemaList,
              timestampColumn,
              valueColumnTypes,
              valueColumns,
              bitMaps,
              columnNameStringList));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
