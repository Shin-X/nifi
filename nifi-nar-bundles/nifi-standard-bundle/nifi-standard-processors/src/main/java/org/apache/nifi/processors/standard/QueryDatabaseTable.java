/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.sql.DefaultAvroSqlWriter;
import org.apache.nifi.processors.standard.sql.SqlWriter;
import org.apache.nifi.util.db.JdbcCommon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.util.db.JdbcProperties.NORMALIZE_NAMES_FOR_AVRO;
import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE;
import static org.apache.nifi.util.db.JdbcProperties.USE_AVRO_LOGICAL_TYPES;


@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "select", "jdbc", "query", "database"})
@SeeAlso({GenerateTableFetch.class, ExecuteSQL.class})
@CapabilityDescription("生成一个SQL select查询，或使用提供的语句，并执行它来获取指定最大值列中的值大于前面看到的最大值的所有行。查询结果将被转换为Avro格式。多个属性支持表达式语言，但不允许传入连接。变量注册表可用于为包含表达式语言的任何属性提供值。如果希望利用流文件属性来执行这些查询，可以使用GenerateTableFetch和/或ExecuteSQL处理器来实现此目的。使用流是为了支持任意大的结果集。可以使用标准的调度方法将该处理器调度为在计时器或cron表达式上运行。这个处理器只能在主节点上运行。“querydbtable.row FlowFile属性。指示被选中的行数。")
@Stateful(scopes = Scope.CLUSTER, description = "After performing a query on the specified table, the maximum values for "
        + "the specified column(s) will be retained for use in future executions of the query. This allows the Processor "
        + "to fetch only those records that have max values greater than the retained values. This can be used for "
        + "incremental fetching, fetching of newly added rows, etc. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation")
@WritesAttributes({
        @WritesAttribute(attribute = "tablename", description="Name of the table being queried"),
        @WritesAttribute(attribute = "querydbtable.row.count", description="The number of rows selected by the query"),
        @WritesAttribute(attribute="fragment.identifier", description="If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute="fragment.index", description="If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "maxvalue.*", description = "Each attribute contains the observed maximum value of a specified 'Maximum-value Column'. The "
                + "suffix of the attribute is the name of the column. If Output Batch Size is set, then this attribute will not be populated.")})
@DynamicProperty(name = "initial.maxvalue.<max_value_column>", value = "Initial maximum value for the specified column",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY, description = "Specifies an initial max value for max value column(s). Properties should "
        + "be added in the format `initial.maxvalue.<max_value_column>`. This value is only used the first time the table is accessed (when a Maximum Value Column is specified).")
@PrimaryNodeOnly
public class QueryDatabaseTable extends AbstractQueryDatabaseTable {

    public QueryDatabaseTable() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(DB_TYPE);
        pds.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(TABLE_NAME)
                .description("The name of the database table to be queried. When a custom query is used, this property is used to alias the query and appears as an attribute on the FlowFile.")
                .build());
        pds.add(COLUMN_NAMES);
        pds.add(WHERE_CLAUSE);
        pds.add(SQL_QUERY);
        pds.add(MAX_VALUE_COLUMN_NAMES);
        pds.add(QUERY_TIMEOUT);
        pds.add(FETCH_SIZE);
        pds.add(MAX_ROWS_PER_FLOW_FILE);
        pds.add(OUTPUT_BATCH_SIZE);
        pds.add(MAX_FRAGMENTS);
        pds.add(NORMALIZE_NAMES_FOR_AVRO);
        pds.add(TRANS_ISOLATION_LEVEL);
        pds.add(USE_AVRO_LOGICAL_TYPES);
        pds.add(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION);
        pds.add(VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE);

        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context) {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final Boolean useAvroLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer defaultPrecision = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION).evaluateAttributeExpressions().asInteger();
        final Integer defaultScale = context.getProperty(VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION).evaluateAttributeExpressions().asInteger();

        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                .recordName(tableName)
                .convertNames(convertNamesForAvro)
                .useLogicalTypes(useAvroLogicalTypes)
                .defaultPrecision(defaultPrecision)
                .defaultScale(defaultScale)
                .maxRows(maxRowsPerFlowFile)
                .build();
        return new DefaultAvroSqlWriter(options);
    }
}
