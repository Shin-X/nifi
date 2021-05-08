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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.checkerframework.checker.units.qual.C;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.copyAttributesToOriginal;

@SideEffectFree
@SupportsBatching
@SeeAlso(PutSQL.class)
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"json", "sql", "database", "rdbms", "insert", "update", "delete", "relational", "flat"})
@CapabilityDescription("将json格式的流文件转换为UPDATE、INSERT或DELETE SQL语句。" +
        "传入的流文件预计是\"flat\"JSON消息，这意味着它由单个JSON元素组成，每个字段映射到一个简单类型。" +
        "如果字段映射到JSON对象，则该JSON对象将被解释为文本。如果输入是一个JSON元素数组，数组中的每个元素都将作为一个单独的流文件输出到“sql”关系。" +
        "成功转换后，原始流文件被路由到“原始”关系，SQL被路由到“SQL”关系。")
//@CapabilityDescription("Converts a JSON-formatted FlowFile into an UPDATE, INSERT, or DELETE SQL statement. The incoming FlowFile is expected to be "
//        + "\"flat\" JSON message, meaning that it consists of a single JSON element and each field maps to a simple type. If a field maps to "
//        + "a JSON object, that JSON object will be interpreted as Text. If the input is an array of JSON elements, each element in the array is "
//        + "output as a separate FlowFile to the 'sql' relationship. Upon successful conversion, the original FlowFile is routed to the 'original' "
//        + "relationship and the SQL is routed to the 'sql' relationship.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets mime.type of FlowFile that is routed to 'sql' to 'text/plain'."),
        @WritesAttribute(attribute = "<sql>.table", description = "Sets the <sql>.table attribute of FlowFile that is routed to 'sql' to the name of the table that is updated by the SQL statement. "
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute = "<sql>.catalog", description = "If the Catalog name is set for this database, specifies the name of the catalog that the SQL statement will update. "
                + "If no catalog is used, this attribute will not be added. The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute = "fragment.identifier", description = "All FlowFiles routed to the 'sql' relationship for the same incoming FlowFile (multiple will be output for the same incoming "
                + "FlowFile if the incoming FlowFile is a JSON Array) will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "The number of SQL FlowFiles that were produced for same incoming FlowFile. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming FlowFile."),
        @WritesAttribute(attribute = "fragment.index", description = "The position of this FlowFile in the list of outgoing FlowFiles that were all derived from the same incoming FlowFile. This can be "
                + "used in conjunction with the fragment.identifier and fragment.count attributes to know which FlowFiles originated from the same incoming FlowFile and in what order the SQL "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "<sql>.args.N.type", description = "The output SQL statements are parametrized in order to avoid SQL Injection Attacks. The types of the Parameters "
                + "to use are stored in attributes named <sql>.args.1.type, <sql>.args.2.type, <sql>.args.3.type, and so on. The type is a number representing a JDBC Type constant. "
                + "Generally, this is useful only for software to read and interpret but is added so that a processor such as PutSQL can understand how to interpret the values. "
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property."),
        @WritesAttribute(attribute = "<sql>.args.N.value", description = "The output SQL statements are parametrized in order to avoid SQL Injection Attacks. The values of the Parameters "
                + "to use are stored in the attributes named sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. Each of these attributes has a corresponding "
                + "<sql>.args.N.type attribute that indicates how the value should be interpreted when inserting it into the database."
                + "The prefix for this attribute ('sql', e.g.) is determined by the SQL Parameter Attribute Prefix property.")
})
public class ConvertJSONToSQL extends AbstractProcessor {
    private static final String UPDATE_TYPE = "UPDATE";
    private static final String INSERT_TYPE = "INSERT";
    private static final String DELETE_TYPE = "DELETE";
    static final String USE_ATTR_TYPE = "Use statement.type Attribute";

    static final String STATEMENT_TYPE_ATTRIBUTE = "statement.type";

    static final AllowableValue IGNORE_UNMATCHED_FIELD = new AllowableValue("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "Any field in the JSON document that cannot be mapped to a column in the database is ignored");
    static final AllowableValue FAIL_UNMATCHED_FIELD = new AllowableValue("Fail", "Fail",
            "If the JSON document has any field that cannot be mapped to a column in the database, the FlowFile will be routed to the failure relationship");
    static final AllowableValue IGNORE_UNMATCHED_COLUMN = new AllowableValue("Ignore Unmatched Columns",
            "Ignore Unmatched Columns",
            "Any column in the database that does not have a field in the JSON document will be assumed to not be required.  No notification will be logged");
    static final AllowableValue WARNING_UNMATCHED_COLUMN = new AllowableValue("Warn on Unmatched Columns",
            "Warn on Unmatched Columns",
            "Any column in the database that does not have a field in the JSON document will be assumed to not be required.  A warning will be logged");
    static final AllowableValue FAIL_UNMATCHED_COLUMN = new AllowableValue("Fail on Unmatched Columns",
            "Fail on Unmatched Columns",
            "A flow will fail if any column in the database that does not have a field in the JSON document.  An error will be logged");

    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC连接池")
            .description("指定用于将JSON消息转换为SQL语句的JDBC连接池。为了确定适当的数据库列类型，连接池是必需的。")
//            .name("JDBC Connection Pool")
//            .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
//                    + "The Connection Pool is necessary in order to determine the appropriate database column types.")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();
    static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("声明类型")
            .description("指定要生成的SQL语句的类型")
//            .name("Statement Type")
//            .description("Specifies the type of SQL Statement to generate")
            .required(true)
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, DELETE_TYPE, USE_ATTR_TYPE)
            .build();
    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("表名")
            .description("语句应该更新的表的名称")
//            .name("Table Name")
//            .description("The name of the table that the statement should update")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("目录名称")
            .description("语句应该更新的目录的名称。这可能不适用于您正在更新的数据库。在本例中，保留字段为空")
//            .name("Catalog Name")
//            .description("The name of the catalog that the statement should update. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("Schema名")
            .description("表所属模式的名称。这可能不适用于您正在更新的数据库。在本例中，保留字段为空")
//            .name("Schema Name")
//            .description("The name of the schema that the table belongs to. This may not apply for the database that you are updating. In this case, leave the field empty")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor TRANSLATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("转化字段名")
            .description("如果为true，处理器将尝试将JSON字段名转换为指定表的适当列名。如果为false，则JSON字段名必须与列名完全匹配，否则不会更新列")
//            .name("Translate Field Names")
//            .description("If true, the Processor will attempt to translate JSON field names into the appropriate column names for the table specified. "
//                    + "If false, the JSON field names must match the column names exactly, or the column will not be updated")
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    static final PropertyDescriptor UNMATCHED_FIELD_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("不匹配字段行为")
            .description("如果传入的JSON元素的字段没有映射到数据库表的任何列，则此属性指定如何处理这种情况")
//            .name("Unmatched Field Behavior")
//            .description("If an incoming JSON element has a field that does not map to any of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_FIELD, FAIL_UNMATCHED_FIELD)
            .defaultValue(IGNORE_UNMATCHED_FIELD.getValue())
            .build();
    static final PropertyDescriptor UNMATCHED_COLUMN_BEHAVIOR = new PropertyDescriptor.Builder()
            .name("不匹配列的行为")
            .description("如果传入的JSON元素没有数据库表所有列的字段映射，则此属性指定如何处理这种情况")
//            .name("Unmatched Column Behavior")
//            .description("If an incoming JSON element does not have a field mapping for all of the database table's columns, this property specifies how to handle the situation")
            .allowableValues(IGNORE_UNMATCHED_COLUMN, WARNING_UNMATCHED_COLUMN, FAIL_UNMATCHED_COLUMN)
            .defaultValue(FAIL_UNMATCHED_COLUMN.getValue())
            .build();
    static final PropertyDescriptor UPDATE_KEY = new PropertyDescriptor.Builder()
            .name("更新 Keys")
            .description("用逗号分隔的列名列表，用于唯一标识数据库中用于UPDATE语句的行。如果语句类型是UPDATE，并且没有设置此属性，则使用表的主键。在这种情况下，如果不存在主键，如果将不匹配的列行为设置为失败，则到SQL的转换将失败。如果语句类型为INSERT，则忽略此属性")
//            .name("Update Keys")
//            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
//                    + "If the Statement Type is UPDATE and this property is not set, the table's Primary Keys are used. "
//                    + "In this case, if no Primary Key exists, the conversion to SQL will fail if Unmatched Column Behaviour is set to FAIL. "
//                    + "This property is ignored if the Statement Type is INSERT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor QUOTED_IDENTIFIERS = new PropertyDescriptor.Builder()
            .name("jts-quoted-identifiers")
            .displayName("引用列标识符")
            .description("启用此选项将导致所有列名都被引用，从而允许您在表中使用保留字作为列名。")
//            .displayName("Quote Column Identifiers")
//            .description("Enabling this option will cause all column names to be quoted, allowing you to "
//                    + "use reserved words as column names in your tables.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor QUOTED_TABLE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("jts-quoted-table-identifiers")
            .displayName("引用表标识符")
            .description("启用此选项将导致表名被引用，以支持在表名中使用特殊字符")
//            .displayName("Quote Table Identifiers")
//            .description("Enabling this option will cause the table name to be quoted to support the "
//                    + "use of special characters in the table name")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor SQL_PARAM_ATTR_PREFIX = new PropertyDescriptor.Builder()
            .name("jts-sql-param-attr-prefix")
            .displayName("SQL参数属性前缀")
            .description("输出流文件属性前的字符串，例如<sql>args.1.value。哪里的<sql>的被替换为指定的值")
//            .displayName("SQL Parameter Attribute Prefix")
//            .description("The string to be prepended to the outgoing flow file attributes, such as <sql>.args.1.value, where <sql> is replaced with the specified value")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .required(true)
            .defaultValue("sql")
            .build();

    static final PropertyDescriptor TABLE_SCHEMA_CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("table-schema-cache-size")
            .displayName("Table Schema 缓存大小")
            .description("指定多少个Table Schema 应该被缓存")
//            .displayName("Table Schema Cache Size")
//            .description("Specifies how many Table Schemas should be cached")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .required(true)
            .build();


    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("原始")
            .description("当一个流文件被转换为SQL时，原始的JSON流文件被路由到这个关系")
            .build();
    static final Relationship REL_SQL = new Relationship.Builder()
            .name("sql")
            .description("当流文件的内容成功转换为SQL语句时，它将被路由到这个关系")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("失败")
            .description("如果不能转换为SQL语句，则将流文件路由到此关系。常见的原因包括无效的JSON内容或JSON内容缺少必需的字段(如果使用INSERT语句类型)。")
            .build();
//    static final Relationship REL_ORIGINAL = new Relationship.Builder()
//            .name("original")
//            .description("When a FlowFile is converted to SQL, the original JSON FlowFile is routed to this relationship")
//            .build();
//    static final Relationship REL_SQL = new Relationship.Builder()
//            .name("sql")
//            .description("A FlowFile is routed to this relationship when its contents have successfully been converted into a SQL statement")
//            .build();
//    static final Relationship REL_FAILURE = new Relationship.Builder()
//            .name("failure")
//            .description("A FlowFile is routed to this relationship if it cannot be converted into a SQL statement. Common causes include invalid JSON "
//                    + "content or the JSON content missing a required field (if using an INSERT statement type).")
//            .build();

    private Cache<SchemaKey, TableSchema> schemaCache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(STATEMENT_TYPE);
        properties.add(TABLE_NAME);
        properties.add(CATALOG_NAME);
        properties.add(SCHEMA_NAME);
        properties.add(TRANSLATE_FIELD_NAMES);
        properties.add(UNMATCHED_FIELD_BEHAVIOR);
        properties.add(UNMATCHED_COLUMN_BEHAVIOR);
        properties.add(UPDATE_KEY);
        properties.add(QUOTED_IDENTIFIERS);
        properties.add(QUOTED_TABLE_IDENTIFIER);
        properties.add(SQL_PARAM_ATTR_PREFIX);
        properties.add(TABLE_SCHEMA_CACHE_SIZE);
        return properties;
    }


    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_ORIGINAL);
        rels.add(REL_SQL);
        rels.add(REL_FAILURE);
        return rels;
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                //.expireAfterWrite(1, TimeUnit.SECONDS)
                .maximumSize(tableSchemaCacheSize)
                .build();
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final boolean translateFieldNames = context.getProperty(TRANSLATE_FIELD_NAMES).asBoolean();
        final boolean ignoreUnmappedFields = IGNORE_UNMATCHED_FIELD.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_FIELD_BEHAVIOR).getValue());
        String statementType = context.getProperty(STATEMENT_TYPE).getValue();
        final String updateKeys = context.getProperty(UPDATE_KEY).evaluateAttributeExpressions(flowFile).getValue();

        final String catalog = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final SchemaKey schemaKey = new SchemaKey(catalog, tableName);
        final boolean includePrimaryKeys = UPDATE_TYPE.equals(statementType) && updateKeys == null;

        // Is the unmatched column behaviour fail or warning?
        final boolean failUnmappedColumns = FAIL_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());
        final boolean warningUnmappedColumns = WARNING_UNMATCHED_COLUMN.getValue().equalsIgnoreCase(context.getProperty(UNMATCHED_COLUMN_BEHAVIOR).getValue());

        //Escape column names?
        final boolean escapeColumnNames = context.getProperty(QUOTED_IDENTIFIERS).asBoolean();

        // Quote table name?
        final boolean quoteTableName = context.getProperty(QUOTED_TABLE_IDENTIFIER).asBoolean();

        // Attribute prefix
        final String attributePrefix = context.getProperty(SQL_PARAM_ATTR_PREFIX).evaluateAttributeExpressions(flowFile).getValue();

        TableSchema schema;
        try {
            schema = schemaCache.get(schemaKey, key -> {
                 DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
                try ( Connection conn = dbcpService.getConnection(flowFile.getAttributes())) {
                    return TableSchema.from(conn, catalog, schemaName, tableName, translateFieldNames, includePrimaryKeys);
                } catch (final SQLException e) {
                    throw new ProcessException(e);
                }
            });
        } catch (ProcessException e) {
            getLogger().error("Failed to convert {} into a SQL statement due to {}; routing to failure", new Object[]{flowFile, e.toString()}, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }


        // Parse the JSON document
        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                        rootNodeRef.set(mapper.readTree(bufferedIn));
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as JSON due to {}; routing to failure", new Object[]{flowFile, pe.toString()}, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final JsonNode rootNode = rootNodeRef.get();
        // The node may or may not be a Json Array. If it isn't, we will create an
        // ArrayNode and add just the root node to it. We do this so that we can easily iterate
        // over the array node, rather than duplicating the logic or creating another function that takes many variables
        // in order to implement the logic.
        final ArrayNode arrayNode;
        if (rootNode.isArray()) {
            arrayNode = (ArrayNode) rootNode;
        } else {
            final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
            arrayNode = new ArrayNode(nodeFactory);
            arrayNode.add(rootNode);
        }

        final String fragmentIdentifier = UUID.randomUUID().toString();

        final Set<FlowFile> created = new HashSet<>();
        //重新获取schame
        schemaCache.cleanUp();
        final int tableSchemaCacheSize = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
        schemaCache = Caffeine.newBuilder()
                .maximumSize(tableSchemaCacheSize)
                .build();
        //重新获取schame
        try {
            schema = schemaCache.get(schemaKey, key -> {
                DBCPService dbcpServiceAfter = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
                try ( Connection connAfter = dbcpServiceAfter.getConnection(flowFile.getAttributes())) {
                    return TableSchema.from(connAfter, catalog, schemaName, tableName, translateFieldNames, includePrimaryKeys);
                } catch (final SQLException e) {
                    throw new ProcessException(e);
                }
            });
        } catch (ProcessException e) {
            getLogger().error("Failed to convert {} into a SQL statement due to {}; routing to failure", new Object[]{flowFile, e.toString()}, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        //对表的梳理
        for (int i = 0; i < arrayNode.size(); i++) {
            final JsonNode jsonNode = arrayNode.get(i);
            //添加列
            if(i==0){
                final Iterator<String> fieldNames = jsonNode.getFieldNames();
                while (fieldNames.hasNext()) {
                    final String fieldName = fieldNames.next();
                    final String fieldNameLowerCase = fieldName.toLowerCase();
                    final ColumnDescription desc = schema.getColumns().get(normalizeColumnName(fieldNameLowerCase, translateFieldNames));
                    if (desc == null) {
                        DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
                        String sql = null;
                        try ( Connection conn = dbcpService.getConnection(flowFile.getAttributes())) {
                            // TableSchema.from(conn, catalog, schemaName, tableName, translateFieldNames, includePrimaryKeys);
                            String type = jsonNode.get(fieldName).getClass().toString();
                            switch (type) {
                                case "class org.codehaus.jackson.node.TextNode":
                                    sql = "alter table " + tableName + " add " + fieldName + " varchar(30)";
                                    System.out.println(sql);
                                    break;
                                case "class org.codehaus.jackson.node.IntNode":
                                    sql = "alter table " + tableName + " add " + fieldName + " int(30)";
                                    break;
                                case "org.codehaus.jackson.node.DoubleNode":
                                    sql = "alter table " + tableName + " add " + fieldName + " double(30)";
                                    break;
                                case "class java.lang.Long":
                                    sql = "alter table " + tableName + " add " + fieldName + " long(30)";
                                    break;
                                case "class java.lang.Float":
                                    sql = "alter table " + tableName + " add " + fieldName + " float(30)";
                                    break;
                            }
                            PreparedStatement preparedStatement = conn.prepareStatement(sql);
                            preparedStatement.executeUpdate();
                            //重新获取schame
                            schemaCache.cleanUp();
                            final int tableSchemaCacheSizeAfter = context.getProperty(TABLE_SCHEMA_CACHE_SIZE).asInteger();
                            schemaCache = Caffeine.newBuilder()
                                    .maximumSize(tableSchemaCacheSizeAfter)
                                    .build();
                            //重新获取schame
                            try {
                                schema = schemaCache.get(schemaKey, key -> {
                                    DBCPService dbcpServiceAfter = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
                                    try ( Connection connAfter = dbcpServiceAfter.getConnection(flowFile.getAttributes())) {
                                        return TableSchema.from(connAfter, catalog, schemaName, tableName, translateFieldNames, includePrimaryKeys);
                                    } catch (final SQLException e) {
                                        throw new ProcessException(e);
                                    }
                                });
                            } catch (ProcessException e) {
                                getLogger().error("Failed to convert {} into a SQL statement due to {}; routing to failure", new Object[]{flowFile, e.toString()}, e);
                                session.transfer(flowFile, REL_FAILURE);
                                return;
                            }
                            conn.close();
                            preparedStatement.close();
                        } catch (final SQLException e) {
                            throw new ProcessException(e);
                        }
                    }
                }
            }


            final String sql;
            final Map<String, String> attributes = new HashMap<>();

            try {
                // build the fully qualified table name
                final StringBuilder tableNameBuilder = new StringBuilder();
                if (catalog != null) {
                    tableNameBuilder.append(catalog).append(".");
                }
                if (schemaName != null) {
                    tableNameBuilder.append(schemaName).append(".");
                }
                tableNameBuilder.append(tableName);
                final String fqTableName = tableNameBuilder.toString();

                if (USE_ATTR_TYPE.equals(statementType)) {
                    statementType = flowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
                }

                if (INSERT_TYPE.equals(statementType)) {
                    sql = generateInsert(jsonNode, attributes, fqTableName, schema, translateFieldNames, ignoreUnmappedFields,
                            failUnmappedColumns, warningUnmappedColumns, escapeColumnNames, quoteTableName, attributePrefix);
                    System.err.println(sql);
                } else if (UPDATE_TYPE.equals(statementType)) {
                    sql = generateUpdate(jsonNode, attributes, fqTableName, updateKeys, schema, translateFieldNames, ignoreUnmappedFields,
                            failUnmappedColumns, warningUnmappedColumns, escapeColumnNames, quoteTableName, attributePrefix);
                } else {
                    sql = generateDelete(jsonNode, attributes, fqTableName, schema, translateFieldNames, ignoreUnmappedFields,
                            failUnmappedColumns, warningUnmappedColumns, escapeColumnNames, quoteTableName, attributePrefix);
                }
            } catch (final ProcessException pe) {
                getLogger().error("Failed to convert {} to a SQL {} statement due to {}; routing to failure",
                        new Object[]{flowFile, statementType, pe.toString()}, pe);
                session.remove(created);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            FlowFile sqlFlowFile = session.create(flowFile);
            created.add(sqlFlowFile);

            sqlFlowFile = session.write(sqlFlowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(sql.getBytes(StandardCharsets.UTF_8));
                }
            });

            attributes.put(CoreAttributes.MIME_TYPE.key(), "text/plain");
            attributes.put(attributePrefix + ".table", tableName);
            attributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
            attributes.put(FRAGMENT_COUNT.key(), String.valueOf(arrayNode.size()));
            attributes.put(FRAGMENT_INDEX.key(), String.valueOf(i));

            if (catalog != null) {
                attributes.put(attributePrefix + ".catalog", catalog);
            }

            sqlFlowFile = session.putAllAttributes(sqlFlowFile, attributes);
            session.transfer(sqlFlowFile, REL_SQL);
        }

        FlowFile newFlowFile = copyAttributesToOriginal(session, flowFile, fragmentIdentifier, arrayNode.size());
        session.transfer(newFlowFile, REL_ORIGINAL);
    }

    private Set<String> getNormalizedColumnNames(final JsonNode node, final boolean translateFieldNames) {
        final Set<String> normalizedFieldNames = new HashSet<>();
        final Iterator<String> fieldNameItr = node.getFieldNames();
        while (fieldNameItr.hasNext()) {
            normalizedFieldNames.add(normalizeColumnName(fieldNameItr.next(), translateFieldNames));
        }

        return normalizedFieldNames;
    }

    private String generateInsert(final JsonNode rootNode, final Map<String, String> attributes, final String tableName,
                                  final TableSchema schema, final boolean translateFieldNames, final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final boolean warningUnmappedColumns, boolean escapeColumnNames, boolean quoteTableName, final String attributePrefix) {

        final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
        for (final String requiredColName : schema.getRequiredColumnNames()) {
            final String normalizedColName = normalizeColumnName(requiredColName, translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "JSON does not have a value for the Required column '" + requiredColName + "'";
                if (failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new ProcessException(missingColMessage);
                } else if (warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("INSERT INTO ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }
        sqlBuilder.append(" (");

        // iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
        // adding the column value to a "<sql>.args.N.value" attribute and the type of a "<sql>.args.N.type" attribute add the
        // columns that we are inserting into
        final Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            String colName = fieldName.toLowerCase();
            final ColumnDescription desc = schema.getColumns().get(normalizeColumnName(colName, translateFieldNames));

            if (desc == null && !ignoreUnmappedFields) {
                throw new ProcessException("Cannot map JSON field '" + fieldName + "' to any column in the database");
            }

            if (desc != null) {
                if (fieldCount++ > 0) {
                    sqlBuilder.append(", ");
                }

                if (escapeColumnNames) {
                    sqlBuilder.append(schema.getQuotedIdentifierString())
                            .append(desc.getColumnName())
                            .append(schema.getQuotedIdentifierString());
                } else {
                    sqlBuilder.append(desc.getColumnName());
                }

                final int sqlType = desc.getDataType();
                attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

                final Integer colSize = desc.getColumnSize();
                final JsonNode fieldNode = rootNode.get(fieldName);
                if (!fieldNode.isNull()) {
                    String fieldValue = createSqlStringValue(fieldNode, colSize, sqlType);
                    attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
                }
            }
        }

        // complete the SQL statements by adding ?'s for all of the values to be escaped.
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < fieldCount; i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }

            sqlBuilder.append("?");
        }
        sqlBuilder.append(")");

        if (fieldCount == 0) {
            throw new ProcessException("None of the fields in the JSON map to the columns defined by the " + tableName + " table");
        }

        return sqlBuilder.toString();
    }

    /**
     * Try to create correct SQL String representation of value.
     */
    protected static String createSqlStringValue(final JsonNode fieldNode, final Integer colSize, final int sqlType) {
        String fieldValue = fieldNode.asText();

        switch (sqlType) {

            // only "true" is considered true, everything else is false
            case Types.BOOLEAN:
                fieldValue = Boolean.valueOf(fieldValue).toString();
                break;

            // Don't truncate numeric types.
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (fieldNode.isBoolean()) {
                    // Convert boolean to number representation for databases those don't support boolean type.
                    fieldValue = fieldNode.asBoolean() ? "1" : "0";
                }
                break;

            // Don't truncate DATE, TIME and TIMESTAMP types. We assume date and time is already correct in long representation.
            // Specifically, milliseconds since January 1, 1970, 00:00:00 GMT
            // However, for TIMESTAMP, PutSQL accepts optional timestamp format via FlowFile attribute.
            // See PutSQL.setParameter method and NIFI-3430 for detail.
            // Alternatively, user can use JSONTreeReader and PutDatabaseRecord to handle date format more efficiently.
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                break;

            // Truncate string data types only.
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                if (colSize != null && fieldValue.length() > colSize) {
                    fieldValue = fieldValue.substring(0, colSize);
                }
                break;
        }

        return fieldValue;
    }

    private String generateUpdate(final JsonNode rootNode, final Map<String, String> attributes, final String tableName, final String updateKeys,
                                  final TableSchema schema, final boolean translateFieldNames, final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final boolean warningUnmappedColumns, boolean escapeColumnNames, boolean quoteTableName, final String attributePrefix) {

        final Set<String> updateKeyNames;
        if (updateKeys == null) {
            updateKeyNames = schema.getPrimaryKeyColumnNames();
        } else {
            updateKeyNames = new HashSet<>();
            for (final String updateKey : updateKeys.split(",")) {
                updateKeyNames.add(updateKey.trim());
            }
        }

        if (updateKeyNames.isEmpty()) {
            throw new ProcessException("Table '" + tableName + "' does not have a Primary Key and no Update Keys were specified");
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("UPDATE ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }

        sqlBuilder.append(" SET ");


        // Create a Set of all normalized Update Key names, and ensure that there is a field in the JSON
        // for each of the Update Key fields.
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
        final Set<String> normalizedUpdateNames = new HashSet<>();
        for (final String uk : updateKeyNames) {
            final String normalizedUK = normalizeColumnName(uk, translateFieldNames);
            normalizedUpdateNames.add(normalizedUK);

            if (!normalizedFieldNames.contains(normalizedUK)) {
                String missingColMessage = "JSON does not have a value for the " + (updateKeys == null ? "Primary" : "Update") + "Key column '" + uk + "'";
                if (failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new ProcessException(missingColMessage);
                } else if (warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        // iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
        // adding the column value to a "<sql>.args.N.value" attribute and the type of a "<sql>.args.N.type" attribute add the
        // columns that we are inserting into
        Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();
            final String normalizedColName = normalizeColumnName(fieldName, translateFieldNames);
            //添加表字段
            final ColumnDescription desc = schema.getColumns().get(normalizedColName);
            if (desc == null) {
                if (!ignoreUnmappedFields) {
                    throw new ProcessException("Cannot map JSON field '" + fieldName + "' to any column in the database");
                } else {
                    continue;
                }
            }

            // Check if this column is an Update Key. If so, skip it for now. We will come
            // back to it after we finish the SET clause
            if (normalizedUpdateNames.contains(normalizedColName)) {
                continue;
            }

            if (fieldCount++ > 0) {
                sqlBuilder.append(", ");
            }

            if (escapeColumnNames) {
                sqlBuilder.append(schema.getQuotedIdentifierString())
                        .append(desc.getColumnName())
                        .append(schema.getQuotedIdentifierString());
            } else {
                sqlBuilder.append(desc.getColumnName());
            }

            sqlBuilder.append(" = ?");
            final int sqlType = desc.getDataType();
            attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

            final Integer colSize = desc.getColumnSize();

            final JsonNode fieldNode = rootNode.get(fieldName);
            if (!fieldNode.isNull()) {
                String fieldValue = createSqlStringValue(fieldNode, colSize, sqlType);
                attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
            }
        }

        // Set the WHERE clause based on the Update Key values
        sqlBuilder.append(" WHERE ");

        fieldNames = rootNode.getFieldNames();
        int whereFieldCount = 0;
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            final String normalizedColName = normalizeColumnName(fieldName, translateFieldNames);
            final ColumnDescription desc = schema.getColumns().get(normalizedColName);
            if (desc == null) {
                continue;
            }

            // Check if this column is a Update Key. If so, skip it for now. We will come
            // back to it after we finish the SET clause
            if (!normalizedUpdateNames.contains(normalizedColName)) {
                continue;
            }

            if (whereFieldCount++ > 0) {
                sqlBuilder.append(" AND ");
            }
            fieldCount++;

            if (escapeColumnNames) {
                sqlBuilder.append(schema.getQuotedIdentifierString())
                        .append(normalizedColName)
                        .append(schema.getQuotedIdentifierString());
            } else {
                sqlBuilder.append(normalizedColName);
            }
            sqlBuilder.append(" = ?");
            final int sqlType = desc.getDataType();
            attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

            final Integer colSize = desc.getColumnSize();
            String fieldValue = rootNode.get(fieldName).asText();
            if (colSize != null && fieldValue.length() > colSize) {
                fieldValue = fieldValue.substring(0, colSize);
            }
            attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
        }

        return sqlBuilder.toString();
    }

    private String generateDelete(final JsonNode rootNode, final Map<String, String> attributes, final String tableName,
                                  final TableSchema schema, final boolean translateFieldNames, final boolean ignoreUnmappedFields, final boolean failUnmappedColumns,
                                  final boolean warningUnmappedColumns, boolean escapeColumnNames, boolean quoteTableName, final String attributePrefix) {
        final Set<String> normalizedFieldNames = getNormalizedColumnNames(rootNode, translateFieldNames);
        for (final String requiredColName : schema.getRequiredColumnNames()) {
            final String normalizedColName = normalizeColumnName(requiredColName, translateFieldNames);
            if (!normalizedFieldNames.contains(normalizedColName)) {
                String missingColMessage = "JSON does not have a value for the Required column '" + requiredColName + "'";
                if (failUnmappedColumns) {
                    getLogger().error(missingColMessage);
                    throw new ProcessException(missingColMessage);
                } else if (warningUnmappedColumns) {
                    getLogger().warn(missingColMessage);
                }
            }
        }

        final StringBuilder sqlBuilder = new StringBuilder();
        int fieldCount = 0;
        sqlBuilder.append("DELETE FROM ");
        if (quoteTableName) {
            sqlBuilder.append(schema.getQuotedIdentifierString())
                    .append(tableName)
                    .append(schema.getQuotedIdentifierString());
        } else {
            sqlBuilder.append(tableName);
        }

        sqlBuilder.append(" WHERE ");

        // iterate over all of the elements in the JSON, building the SQL statement by adding the column names, as well as
        // adding the column value to a "<sql>.args.N.value" attribute and the type of a "<sql>.args.N.type" attribute add the
        // columns that we are inserting into
        final Iterator<String> fieldNames = rootNode.getFieldNames();
        while (fieldNames.hasNext()) {
            final String fieldName = fieldNames.next();

            final ColumnDescription desc = schema.getColumns().get(normalizeColumnName(fieldName, translateFieldNames));
            if (desc == null && !ignoreUnmappedFields) {
                throw new ProcessException("Cannot map JSON field '" + fieldName + "' to any column in the database");
            }

            if (desc != null) {
                if (fieldCount++ > 0) {
                    sqlBuilder.append(" AND ");
                }

                if (escapeColumnNames) {
                    sqlBuilder.append(schema.getQuotedIdentifierString())
                            .append(desc.getColumnName())
                            .append(schema.getQuotedIdentifierString());
                } else {
                    sqlBuilder.append(desc.getColumnName());
                }
                sqlBuilder.append(" = ?");

                final int sqlType = desc.getDataType();
                attributes.put(attributePrefix + ".args." + fieldCount + ".type", String.valueOf(sqlType));

                final Integer colSize = desc.getColumnSize();
                final JsonNode fieldNode = rootNode.get(fieldName);
                if (!fieldNode.isNull()) {
                    String fieldValue = fieldNode.asText();
                    if (colSize != null && fieldValue.length() > colSize) {
                        fieldValue = fieldValue.substring(0, colSize);
                    }
                    attributes.put(attributePrefix + ".args." + fieldCount + ".value", fieldValue);
                }
            }
        }

        if (fieldCount == 0) {
            throw new ProcessException("None of the fields in the JSON map to the columns defined by the " + tableName + " table");
        }

        return sqlBuilder.toString();
    }

    private static String normalizeColumnName(final String colName, final boolean translateColumnNames) {
        return translateColumnNames ? colName.toUpperCase().replace("_", "") : colName;
    }

    private static class TableSchema {
        private List<String> requiredColumnNames;
        private Set<String> primaryKeyColumnNames;
        private Map<String, ColumnDescription> columns;
        private String quotedIdentifierString;

        private TableSchema(final List<ColumnDescription> columnDescriptions, final boolean translateColumnNames,
                            final Set<String> primaryKeyColumnNames, final String quotedIdentifierString) {
            this.columns = new HashMap<>();
            this.primaryKeyColumnNames = primaryKeyColumnNames;
            this.quotedIdentifierString = quotedIdentifierString;

            this.requiredColumnNames = new ArrayList<>();
            for (final ColumnDescription desc : columnDescriptions) {
                columns.put(ConvertJSONToSQL.normalizeColumnName(desc.columnName, translateColumnNames), desc);
                if (desc.isRequired()) {
                    requiredColumnNames.add(desc.columnName);
                }
            }
        }

        public Map<String, ColumnDescription> getColumns() {
            return columns;
        }

        public List<String> getRequiredColumnNames() {
            return requiredColumnNames;
        }

        public Set<String> getPrimaryKeyColumnNames() {
            return primaryKeyColumnNames;
        }

        public String getQuotedIdentifierString() {
            return quotedIdentifierString;
        }

        public static TableSchema from(final Connection conn, final String catalog, final String schema, final String tableName,
                                       final boolean translateColumnNames, final boolean includePrimaryKeys) throws SQLException {
            final DatabaseMetaData dmd = conn.getMetaData();

            try (final ResultSet colrs = dmd.getColumns(catalog, schema, tableName, "%")) {
                final List<ColumnDescription> cols = new ArrayList<>();
                while (colrs.next()) {
                    final ColumnDescription col = ColumnDescription.from(colrs);
                    cols.add(col);
                }

                final Set<String> primaryKeyColumns = new HashSet<>();
                if (includePrimaryKeys) {
                    try (final ResultSet pkrs = conn.getMetaData().getPrimaryKeys(catalog, null, tableName)) {

                        while (pkrs.next()) {
                            final String colName = pkrs.getString("COLUMN_NAME");
                            primaryKeyColumns.add(normalizeColumnName(colName, translateColumnNames));
                        }
                    }
                }

                return new TableSchema(cols, translateColumnNames, primaryKeyColumns, dmd.getIdentifierQuoteString());
            }
        }



    }

    private static class ColumnDescription {
        private final String columnName;
        private final int dataType;
        private final boolean required;
        private final Integer columnSize;

        private ColumnDescription(final String columnName, final int dataType, final boolean required, final Integer columnSize) {
            this.columnName = columnName;
            this.dataType = dataType;
            this.required = required;
            this.columnSize = columnSize;
        }

        public int getDataType() {
            return dataType;
        }

        public Integer getColumnSize() {
            return columnSize;
        }

        public String getColumnName() {
            return columnName;
        }

        public boolean isRequired() {
            return required;
        }

        public static ColumnDescription from(final ResultSet resultSet) throws SQLException {
            final ResultSetMetaData md = resultSet.getMetaData();
            List<String> columns = new ArrayList<>();

            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                columns.add(md.getColumnName(i));
            }
            // COLUMN_DEF must be read first to work around Oracle bug, see NIFI-4279 for details
            final String defaultValue = resultSet.getString("COLUMN_DEF");
            final String columnName = resultSet.getString("COLUMN_NAME");
            final int dataType = resultSet.getInt("DATA_TYPE");
            final int colSize = resultSet.getInt("COLUMN_SIZE");

            final String nullableValue = resultSet.getString("IS_NULLABLE");
            final boolean isNullable = "YES".equalsIgnoreCase(nullableValue) || nullableValue.isEmpty();

            String autoIncrementValue = "NO";

            if (columns.contains("IS_AUTOINCREMENT")) {
                autoIncrementValue = resultSet.getString("IS_AUTOINCREMENT");
            }

            final boolean isAutoIncrement = "YES".equalsIgnoreCase(autoIncrementValue);
            final boolean required = !isNullable && !isAutoIncrement && defaultValue == null;

            return new ColumnDescription(columnName, dataType, required, colSize == 0 ? null : colSize);
        }
    }

    private static class SchemaKey {
        private final String catalog;
        private final String tableName;

        public SchemaKey(final String catalog, final String tableName) {
            this.catalog = catalog;
            this.tableName = tableName;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
            result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            final SchemaKey other = (SchemaKey) obj;
            if (catalog == null) {
                if (other.catalog != null) {
                    return false;
                }
            } else if (!catalog.equals(other.catalog)) {
                return false;
            }


            if (tableName == null) {
                if (other.tableName != null) {
                    return false;
                }
            } else if (!tableName.equals(other.tableName)) {
                return false;
            }

            return true;
        }
    }
}
