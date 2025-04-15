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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.pipe.connector.protocol.writeback.WriteBackConnector;
import org.apache.iotdb.db.pipe.event.common.row.PipeResetTabletRow;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.protocol.rest.table.v1.handler.QueryDataSetHandler;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.utils.SetThreadName;
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

import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Optional;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_EXPR;

@TreeModel
@TableModel
public class ExpressionProcessor implements PipeProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionProcessor.class);

  private static final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
  private static final SqlParser sqlParser = new SqlParser();

  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private static final SessionManager SESSION_MANAGER = SessionManager.getInstance();
  private IClientSession treeSession;

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

    treeSession =
            new InternalClientSession(
                    String.format(
                            "%s_%s_%s_%s_tree",
                            WriteBackConnector.class.getSimpleName(),
                            environment.getPipeName(),
                            environment.getCreationTime(),
                            environment.getRegionId()));
    treeSession.setUsername(AuthorityChecker.SUPER_USER);
    treeSession.setClientVersion(IoTDBConstant.ClientVersion.V_1_0);
    treeSession.setZoneId(ZoneId.systemDefault());

    executeShowSeriesForTreeModel(dataBaseName);

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
    if (treeSession != null) {
      SESSION_MANAGER.closeSession(treeSession, COORDINATOR::cleanupQueryExecution);
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

  private void executeShowSeriesForTreeModel(final String databaseName) {
    String fullDeviceName = databaseName + TsFileConstant.PATH_SEPARATOR + "t1" + ".*";
    Long queryId = null;
    try {
      Statement showTimeSeriesStatement =
              new ShowTimeSeriesStatement(new PartialPath(fullDeviceName), false);

      treeSession.setDatabaseName(databaseName);
      treeSession.setSqlDialect(IClientSession.SqlDialect.TREE);
      SESSION_MANAGER.registerSession(treeSession);

      queryId = SESSION_MANAGER.requestQueryId();
      ExecutionResult result = Coordinator.getInstance()
              .executeForTreeModel(
                      showTimeSeriesStatement,
                      queryId,
                      SESSION_MANAGER.getSessionInfo(treeSession),
                      "",
                      ClusterPartitionFetcher.getInstance(),
                      ClusterSchemaFetcher.getInstance(),
                      IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold(),
                      false);
      if (result.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()
              && result.status.code != TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()
              && result.status.code != TSStatusCode.DATABASE_CONFLICT.getStatusCode()) {
        LOGGER.error(
                "Show TimeSeries error, statement: {}, result status : {}.",
                showTimeSeriesStatement, result.status);
      }

      IQueryExecution queryExecution = COORDINATOR.getQueryExecution(queryId);
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (optionalTsBlock.isPresent()) {
        TsBlock tsBlock = optionalTsBlock.get();
        int rowCount = tsBlock.getPositionCount();
        columnNameStringList = new String[rowCount];
        measurementSchemaList = new MeasurementSchema[rowCount];
        valueColumnTypes = new TSDataType[rowCount];

        for(int i = 0; i < rowCount; i++) {
          // Timeseries
          String[] fullName = tsBlock.getColumn(0).getBinary(i).toString().split("[.]");
          columnNameStringList[i] = fullName[fullName.length - 1];
                  ;
          // Column Type
          String columnType = tsBlock.getColumn(3).getBinary(i).toString();
          valueColumnTypes[i] = getTsDataType(columnType);
          // Measurement
          measurementSchemaList[i] = new MeasurementSchema(
                 columnNameStringList[i], valueColumnTypes[i]
          );
        }
      }
    } catch (IoTDBException e) {
      throw new RuntimeException(e);
    } finally {
      if (queryId != null) {
        COORDINATOR.cleanupQueryExecution(queryId);
      }
    }
  }

  private TSDataType getTsDataType(String tsType) {
    switch (tsType) {
      case "INT32":
        return TSDataType.INT32;
      case "FLOAT":
        return TSDataType.FLOAT;
      default:
        return null;
    }
  }
}
