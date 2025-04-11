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
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.security.AccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.security.AllowAllAccessControl;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import java.io.IOException;
import java.time.ZoneId;

import static org.apache.iotdb.commons.pipe.config.constant.PipeProcessorConstant.PROCESSOR_EXPR;

@TableModel
public class ExpressionProcessor implements PipeProcessor {
  private static final SqlParser RELATIONAL_SQL_PARSER = new SqlParser();

  private final AccessControl nopAccessControl = new AllowAllAccessControl();

  private final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();
  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
  private final Metadata metadata = LocalExecutionPlanner.getInstance().metadata;
  private String sql;
  private Expression expression;

  private String dataBaseName;
  private Boolean isTableModel;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(PROCESSOR_EXPR);
    final PipeParameters parameters = validator.getParameters();
    sql = parameters.getString(PROCESSOR_EXPR).toLowerCase();
    expression = RELATIONAL_SQL_PARSER.createExpression(sql, ZoneId.systemDefault());

    // bind & check
    //    StatementAnalyzerFactory statementAnalyzerFactory =
    //            new StatementAnalyzerFactory(metadata, relationSqlParser, nopAccessControl);
    //    QueryId queryId = new QueryId("expression_processor");
    //    SessionInfo session =
    //            new SessionInfo(
    //                    1L,
    //                    "iotdb-user",
    //                    ZoneId.systemDefault(),
    //                    IoTDBConstant.ClientVersion.V_1_0,
    //                    "db1",
    //                    IClientSession.SqlDialect.TABLE);
    //    MPPQueryContext context = new MPPQueryContext(sql, queryId, session, null, null);
    //    Analysis analysis = new Analysis(null, Collections.emptyMap());
    //
    //    Scope scope = Scope.create();
    //    Scope sourceScope =
    //
    //    ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
    //            metadata,
    //            context,
    //            session,
    //            statementAnalyzerFactory,
    //            nopAccessControl,
    //            scope,
    //            analysis,
    //            expression,
    //            WarningCollector.NOOP,
    //            CorrelationSupport.ALLOWED);
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    final PipeRuntimeEnvironment environment = configuration.getRuntimeEnvironment();
    dataBaseName =
        StorageEngine.getInstance()
            .getDataRegion(new DataRegionId(environment.getRegionId()))
            .getDatabaseName();
    if (dataBaseName != null) {
      isTableModel = PathUtils.isTableModelDatabase(dataBaseName);
    }
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    System.out.println("process TabletInsertionEvent: " + sql);
    tabletInsertionEvent.processTablet((tablet, rowCollector) -> tablet.getTableName());
    eventCollector.collect(tabletInsertionEvent);
  }

  @Override
  public void process(TsFileInsertionEvent tsFileInsertionEvent, EventCollector eventCollector)
      throws IOException {
    eventCollector.collect(tsFileInsertionEvent);
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    System.out.println("process event expr: " + sql);
    eventCollector.collect(event);
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }
}
