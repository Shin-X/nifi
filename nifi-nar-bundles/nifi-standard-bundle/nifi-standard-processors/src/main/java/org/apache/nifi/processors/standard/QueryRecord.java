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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.queryrecord.FlowFileTable;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_PRECISION;
import static org.apache.nifi.util.db.JdbcProperties.DEFAULT_SCALE;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"sql", "query", "calcite", "route", "record", "transform", "select", "update", "modify", "etl", "filter", "record", "csv", "json", "logs", "text", "avro", "aggregate"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("对一个流文件的内容计算一个或多个SQL查询。然后，SQL查询的结果成为输出流文件的内容。例如，这可以用于特定于字段的过滤、转换和行级过滤。可以重命名列，执行简单的计算和聚合，等等。处理器配置有记录读取器控制器服务和记录写入器服务，以便允许传入和传出数据格式的灵活性。处理器必须配置至少一个用户定义的属性。属性的名称是要将数据路由到的关系，属性的值是一个SQL SELECT语句，用于指定如何转换/筛选输入数据。SQL语句必须是有效的ANSI SQL，并且由Apache方解石提供支持。如果转换失败，原始的流文件将被路由到“失败”关系。否则，所选数据将被路由到关联关系。如果记录编写者选择从记录继承模式，请注意，继承的模式将来自ResultSet，而不是输入记录，这一点很重要。这允许QueryRecord处理器的单个实例具有多个查询，每个查询返回一组不同的列和聚合。然而，结果是，派生的模式将没有模式名，因此，如果从记录继承模式，配置的记录写入器不尝试将模式名写入属性是很重要的。有关更多信息，请参阅处理器使用文档。")
@DynamicRelationship(name="<Property Name>", description="Each user-defined property defines a new Relationship for this Processor.")
@DynamicProperty(name = "The name of the relationship to route data to",
                 value="A SQL SELECT statement that is used to determine what data should be routed to this relationship.",
                 expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                 description="Each user-defined property specifies a SQL SELECT statement to run over the data, with the data "
                         + "that is selected being routed to the relationship whose name is the property name")
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records selected by the query")
})
public class QueryRecord extends AbstractProcessor {
    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER_FACTORY = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing results to a FlowFile")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
        .name("include-zero-record-flowfiles")
        .displayName("Include Zero Record FlowFiles")
        .description("When running the SQL statement against an incoming FlowFile, if the result has no data, "
            + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    static final PropertyDescriptor CACHE_SCHEMA = new PropertyDescriptor.Builder()
        .name("cache-schema")
        .displayName("Cache Schema")
        .description("This property is no longer used. It remains solely for backward compatibility in order to avoid making existing Processors invalid upon upgrade. This property will be" +
            " removed in future versions. Now, instead of forcing the user to understand the semantics of schema caching, the Processor caches up to 25 schemas and automatically rolls off the" +
            " old schemas. This provides the same performance when caching was enabled previously and in some cases very significant performance improvements if caching was previously disabled.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The original FlowFile is routed to this relationship")
        .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile fails processing for any reason (for example, the SQL "
            + "statement contains columns not present in input data), the original FlowFile it will "
            + "be routed to this relationship")
        .build();

    private List<PropertyDescriptor> properties;
    private final Set<Relationship> relationships = Collections.synchronizedSet(new HashSet<>());

    private final Cache<Tuple<String, RecordSchema>, BlockingQueue<CachedStatement>> statementQueues = Caffeine.newBuilder()
        .maximumSize(25)
        .removalListener(this::onCacheEviction)
        .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        try {
            DriverManager.registerDriver(new org.apache.calcite.jdbc.Driver());
        } catch (final SQLException e) {
            throw new ProcessException("Failed to load Calcite JDBC Driver", e);
        }

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER_FACTORY);
        properties.add(RECORD_WRITER_FACTORY);
        properties.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        properties.add(CACHE_SCHEMA);
        properties.add(DEFAULT_PRECISION);
        properties.add(DEFAULT_SCALE);
        this.properties = Collections.unmodifiableList(properties);

        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }

        final Relationship relationship = new Relationship.Builder()
            .name(descriptor.getName())
            .description("User-defined relationship that specifies where data that matches the specified SQL query should be routed")
            .build();

        if (newValue == null) {
            relationships.remove(relationship);
        } else {
            relationships.add(relationship);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("SQL select statement specifies how data should be filtered/transformed. "
                + "SQL SELECT should select from the FLOWFILE table")
            .required(false)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(new SqlValidator())
            .build();
    }

    @OnStopped
    public synchronized void cleanup() {
        for (final BlockingQueue<CachedStatement> statementQueue : statementQueues.asMap().values()) {
            clearQueue(statementQueue);
        }

        statementQueues.invalidateAll();
    }

    private void onCacheEviction(final Tuple<String, RecordSchema> key, final BlockingQueue<CachedStatement> queue, final RemovalCause cause) {
        clearQueue(queue);
    }

    private void clearQueue(final BlockingQueue<CachedStatement> statementQueue) {
        CachedStatement stmt;
        while ((stmt = statementQueue.poll()) != null) {
            closeQuietly(stmt.getStatement(), stmt.getConnection());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);

        final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(RECORD_WRITER_FACTORY).asControllerService(RecordSetWriterFactory.class);
        final RecordReaderFactory recordReaderFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        final Integer defaultPrecision = context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions(original).asInteger();
        final Integer defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions(original).asInteger();

        final Map<FlowFile, Relationship> transformedFlowFiles = new HashMap<>();
        final Set<FlowFile> createdFlowFiles = new HashSet<>();

        // Determine the Record Reader's schema
        final RecordSchema writerSchema;
        final RecordSchema readerSchema;
        try (final InputStream rawIn = session.read(original)) {
            final Map<String, String> originalAttributes = original.getAttributes();
            final RecordReader reader = recordReaderFactory.createRecordReader(originalAttributes, rawIn, original.getSize(), getLogger());
            readerSchema = reader.getSchema();

            writerSchema = recordSetWriterFactory.getSchema(originalAttributes, readerSchema);
        } catch (final Exception e) {
            getLogger().error("Failed to determine Record Schema from {}; routing to failure", new Object[] {original, e});
            session.transfer(original, REL_FAILURE);
            return;
        }

        // Determine the schema for writing the data
        final Map<String, String> originalAttributes = original.getAttributes();
        int recordsRead = 0;

        try {
            for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
                if (!descriptor.isDynamic()) {
                    continue;
                }

                final Relationship relationship = new Relationship.Builder().name(descriptor.getName()).build();

                // We have to fork a child because we may need to read the input FlowFile more than once,
                // and we cannot call session.read() on the original FlowFile while we are within a write
                // callback for the original FlowFile.
                FlowFile transformed = session.create(original);
                boolean flowFileRemoved = false;

                try {
                    final String sql = context.getProperty(descriptor).evaluateAttributeExpressions(original).getValue();
                    final AtomicReference<WriteResult> writeResultRef = new AtomicReference<>();
                    final QueryResult queryResult = query(session, original, readerSchema, sql, recordReaderFactory);

                    final AtomicReference<String> mimeTypeRef = new AtomicReference<>();
                    try {
                        final ResultSet rs = queryResult.getResultSet();
                        transformed = session.write(transformed, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream out) throws IOException {
                                final ResultSetRecordSet recordSet;
                                final RecordSchema writeSchema;

                                try {
                                    recordSet = new ResultSetRecordSet(rs, writerSchema, defaultPrecision, defaultScale);
                                    final RecordSchema resultSetSchema = recordSet.getSchema();
                                    writeSchema = recordSetWriterFactory.getSchema(originalAttributes, resultSetSchema);
                                } catch (final SQLException | SchemaNotFoundException e) {
                                    throw new ProcessException(e);
                                }

                                try (final RecordSetWriter resultSetWriter = recordSetWriterFactory.createWriter(getLogger(), writeSchema, out, original)) {
                                    writeResultRef.set(resultSetWriter.write(recordSet));
                                    mimeTypeRef.set(resultSetWriter.getMimeType());
                                } catch (final Exception e) {
                                    throw new IOException(e);
                                }
                            }
                        });
                    } finally {
                        closeQuietly(queryResult);
                    }

                    recordsRead = Math.max(recordsRead, queryResult.getRecordsRead());
                    final WriteResult result = writeResultRef.get();
                    if (result.getRecordCount() == 0 && !context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean()) {
                        session.remove(transformed);
                        flowFileRemoved = true;
                        transformedFlowFiles.remove(transformed);
                        getLogger().info("Transformed {} but the result contained no data so will not pass on a FlowFile", new Object[] {original});
                    } else {
                        final Map<String, String> attributesToAdd = new HashMap<>();
                        if (result.getAttributes() != null) {
                            attributesToAdd.putAll(result.getAttributes());
                        }

                        attributesToAdd.put(CoreAttributes.MIME_TYPE.key(), mimeTypeRef.get());
                        attributesToAdd.put("record.count", String.valueOf(result.getRecordCount()));
                        transformed = session.putAllAttributes(transformed, attributesToAdd);
                        transformedFlowFiles.put(transformed, relationship);

                        session.adjustCounter("Records Written", result.getRecordCount(), false);
                    }
                } finally {
                    // Ensure that we have the FlowFile in the set in case we throw any Exception
                    if (!flowFileRemoved) {
                        createdFlowFiles.add(transformed);
                    }
                }
            }

            final long elapsedMillis = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            if (transformedFlowFiles.size() > 0) {
                session.getProvenanceReporter().fork(original, transformedFlowFiles.keySet(), elapsedMillis);

                for (final Map.Entry<FlowFile, Relationship> entry : transformedFlowFiles.entrySet()) {
                    final FlowFile transformed = entry.getKey();
                    final Relationship relationship = entry.getValue();

                    session.getProvenanceReporter().route(transformed, relationship);
                    session.transfer(transformed, relationship);
                }
            }

            getLogger().info("Successfully queried {} in {} millis", new Object[] {original, elapsedMillis});
            session.transfer(original, REL_ORIGINAL);
        } catch (final SQLException e) {
            getLogger().error("Unable to query {} due to {}", new Object[] {original, e.getCause() == null ? e : e.getCause()});
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);
        } catch (final Exception e) {
            getLogger().error("Unable to query {} due to {}", new Object[] {original, e});
            session.remove(createdFlowFiles);
            session.transfer(original, REL_FAILURE);
        }

        session.adjustCounter("Records Read", recordsRead, false);
    }


    private synchronized CachedStatement getStatement(final String sql, final RecordSchema schema, final Supplier<CachedStatement> statementBuilder) {
        final Tuple<String, RecordSchema> tuple = new Tuple<>(sql, schema);
        final BlockingQueue<CachedStatement> statementQueue = statementQueues.get(tuple, key -> new LinkedBlockingQueue<>());

        final CachedStatement cachedStmt = statementQueue.poll();
        if (cachedStmt != null) {
            return cachedStmt;
        }

        return statementBuilder.get();
    }

    private CachedStatement buildCachedStatement(final String sql, final ProcessSession session,  final FlowFile flowFile, final RecordSchema schema,
                                                 final RecordReaderFactory recordReaderFactory) {

        final CalciteConnection connection = createConnection();
        final SchemaPlus rootSchema = createRootSchema(connection);

        final FlowFileTable flowFileTable = new FlowFileTable(session, flowFile, schema, recordReaderFactory, getLogger());
        rootSchema.add("FLOWFILE", flowFileTable);
        rootSchema.setCacheEnabled(false);

        try {
            final PreparedStatement stmt = connection.prepareStatement(sql);
            return new CachedStatement(stmt, flowFileTable, connection);
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }


    private CalciteConnection createConnection() {
        final Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.LEX.camelName(), Lex.MYSQL_ANSI.name());

        try {
            final Connection connection = DriverManager.getConnection("jdbc:calcite:", properties);
            final CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            return calciteConnection;
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }


    protected QueryResult query(final ProcessSession session, final FlowFile flowFile, final RecordSchema schema, final String sql, final RecordReaderFactory recordReaderFactory)
                throws SQLException {

        final Supplier<CachedStatement> statementBuilder = () -> buildCachedStatement(sql, session, flowFile, schema, recordReaderFactory);

        final CachedStatement cachedStatement = getStatement(sql, schema, statementBuilder);
        final PreparedStatement stmt = cachedStatement.getStatement();
        final FlowFileTable table = cachedStatement.getTable();
        table.setFlowFile(session, flowFile);

        final ResultSet rs;
        try {
            rs = stmt.executeQuery();
        } catch (final Throwable t) {
            table.close();
            throw t;
        }

        return new QueryResult() {
            @Override
            public void close() throws IOException {
                table.close();

                final BlockingQueue<CachedStatement> statementQueue = statementQueues.getIfPresent(new Tuple<>(sql, schema));
                if (statementQueue == null || !statementQueue.offer(cachedStatement)) {
                    try {
                        cachedStatement.getConnection().close();
                    } catch (SQLException e) {
                        throw new IOException("Failed to close statement", e);
                    }
                }
            }

            @Override
            public ResultSet getResultSet() {
                return rs;
            }

            @Override
            public int getRecordsRead() {
                return table.getRecordsRead();
            }

        };
    }

    private SchemaPlus createRootSchema(final CalciteConnection calciteConnection) {
        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("RPATH", ScalarFunctionImpl.create(ObjectRecordPath.class, "eval"));
        rootSchema.add("RPATH_STRING", ScalarFunctionImpl.create(StringRecordPath.class, "eval"));
        rootSchema.add("RPATH_INT", ScalarFunctionImpl.create(IntegerRecordPath.class, "eval"));
        rootSchema.add("RPATH_LONG", ScalarFunctionImpl.create(LongRecordPath.class, "eval"));
        rootSchema.add("RPATH_DATE", ScalarFunctionImpl.create(DateRecordPath.class, "eval"));
        rootSchema.add("RPATH_DOUBLE", ScalarFunctionImpl.create(DoubleRecordPath.class, "eval"));
        rootSchema.add("RPATH_FLOAT", ScalarFunctionImpl.create(FloatRecordPath.class, "eval"));

        return rootSchema;
    }

    private void closeQuietly(final AutoCloseable... closeables) {
        if (closeables == null) {
            return;
        }

        for (final AutoCloseable closeable : closeables) {
            if (closeable == null) {
                continue;
            }

            try {
                closeable.close();
            } catch (final Exception e) {
                getLogger().warn("Failed to close SQL resource", e);
            }
        }
    }

    private static class SqlValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder()
                    .input(input)
                    .subject(subject)
                    .valid(true)
                    .explanation("Expression Language Present")
                    .build();
            }

            final String substituted = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();

            final Config config = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .build();

            final SqlParser parser = SqlParser.create(substituted, config);
            try {
                parser.parseStmt();
                return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(true)
                    .build();
            } catch (final Exception e) {
                return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Not a valid SQL Statement: " + e.getMessage())
                    .build();
            }
        }
    }

    private interface QueryResult extends Closeable {
        ResultSet getResultSet();

        int getRecordsRead();
    }

    private static class CachedStatement {
        private final FlowFileTable table;
        private final PreparedStatement statement;
        private final Connection connection;

        public CachedStatement(final PreparedStatement statement, final FlowFileTable table, final Connection connection) {
            this.statement = statement;
            this.table = table;
            this.connection = connection;
        }

        public FlowFileTable getTable() {
            return table;
        }

        public PreparedStatement getStatement() {
            return statement;
        }

        public Connection getConnection() {
            return connection;
        }
    }


    // ------------------------------------------------------------
    // User-Defined Functions for Calcite
    // ------------------------------------------------------------


    public static class ObjectRecordPath extends RecordPathFunction {
        private static final RecordField ROOT_RECORD_FIELD = new RecordField("root", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
        private static final RecordSchema ROOT_RECORD_SCHEMA = new SimpleRecordSchema(Collections.singletonList(ROOT_RECORD_FIELD));
        private static final RecordField PARENT_RECORD_FIELD = new RecordField("root", RecordFieldType.RECORD.getRecordDataType(ROOT_RECORD_SCHEMA));


        public Object eval(Object record, String recordPath) {
            if (record == null) {
                return null;
            }
            if (record instanceof Record) {
                return eval((Record) record, recordPath);
            }
            if (record instanceof Record[]) {
                return eval((Record[]) record, recordPath);
            }
            if (record instanceof Iterable) {
                return eval((Iterable<Record>) record, recordPath);
            }
            if (record instanceof Map) {
                return eval((Map<?, ?>) record, recordPath);
            }

            throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " against given argument because the argument is of type " + record.getClass() + " instead of Record");
        }

        private Object eval(final Map<?, ?> map, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final Record record = new MapRecord(ROOT_RECORD_SCHEMA, Collections.singletonMap("root", map));
            final FieldValue parentFieldValue = new StandardFieldValue(record, PARENT_RECORD_FIELD, null);
            final FieldValue fieldValue = new StandardFieldValue(map, ROOT_RECORD_FIELD, parentFieldValue);
            final RecordPathResult result = compiled.evaluate(record, fieldValue);

            final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());
            return evalResults(selectedFields);
        }

        private Object eval(final Record record, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);
            final RecordPathResult result = compiled.evaluate(record);

            final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());
            return evalResults(selectedFields);
        }

        private Object eval(final Iterable<Record> records, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final List<FieldValue> selectedFields = new ArrayList<>();
            for (final Record record : records) {
                final RecordPathResult result = compiled.evaluate(record);
                result.getSelectedFields().forEach(selectedFields::add);
            }

            return evalResults(selectedFields);
        }

        private Object eval(final Record[] records, final String recordPath) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final List<FieldValue> selectedFields = new ArrayList<>();
            for (final Record record : records) {
                final RecordPathResult result = compiled.evaluate(record);
                result.getSelectedFields().forEach(selectedFields::add);
            }

            return evalResults(selectedFields);
        }

        private Object evalResults(final List<FieldValue> selectedFields) {
            if (selectedFields.isEmpty()) {
                return null;
            }

            if (selectedFields.size() == 1) {
                return selectedFields.get(0).getValue();
            }

            return selectedFields.stream()
                .map(FieldValue::getValue)
                .toArray();
        }

    }

    public static class StringRecordPath extends RecordPathFunction {
        public String eval(Object record, String recordPath) {
            return eval(record, recordPath, Object::toString);
        }
    }

    public static class IntegerRecordPath extends RecordPathFunction {
        public Integer eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).intValue();
                }
                if (val instanceof String) {
                    return Integer.parseInt((String) val);
                }
                if (val instanceof Date) {
                    return (int) ((Date) val).getTime();
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Integer against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class LongRecordPath extends RecordPathFunction {
        public Long eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).longValue();
                }
                if (val instanceof String) {
                    return Long.parseLong((String) val);
                }
                if (val instanceof Date) {
                    return ((Date) val).getTime();
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Long against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class FloatRecordPath extends RecordPathFunction {
        public Float eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).floatValue();
                }
                if (val instanceof String) {
                    return Float.parseFloat((String) val);
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Float against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class DoubleRecordPath extends RecordPathFunction {
        public Double eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).doubleValue();
                }
                if (val instanceof String) {
                    return Double.parseDouble((String) val);
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Double against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }

    public static class DateRecordPath extends RecordPathFunction {
        // Interestingly, Calcite throws an Exception if the schema indicates a DATE type and we return a java.util.Date. Calcite requires that a Long be returned instead.
        public Long eval(Object record, String recordPath) {
            return eval(record, recordPath, val -> {
                if (val instanceof Number) {
                    return ((Number) val).longValue();
                }
                if (val instanceof String) {
                    throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Date against " + record
                        + " because the value returned is of type String. To parse a String value as a Date, please use the toDate function. For example, " +
                        "SELECT RPATH_DATE( record, 'toDate( /event/timestamp, \"yyyy-MM-dd\" )' ) AS eventDate FROM FLOWFILE");
                }
                if (val instanceof Date) {
                    return ((Date) val).getTime();
                }

                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " as Date against " + record
                    + " because the value returned is of type " + val.getClass());
            });
        }
    }


    public static class RecordPathFunction {
        private static final RecordField ROOT_RECORD_FIELD = new RecordField("root", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
        private static final RecordSchema ROOT_RECORD_SCHEMA = new SimpleRecordSchema(Collections.singletonList(ROOT_RECORD_FIELD));
        private static final RecordField PARENT_RECORD_FIELD = new RecordField("root", RecordFieldType.RECORD.getRecordDataType(ROOT_RECORD_SCHEMA));

        protected static final RecordPathCache RECORD_PATH_CACHE = new RecordPathCache(100);

        protected <T> T eval(final Object record, final String recordPath, final Function<Object, T> transform) {
            if (record == null) {
                return null;
            }

            try {
                if (record instanceof Record) {
                    return eval((Record) record, recordPath, transform);
                } else if (record instanceof Record[]) {
                    return eval((Record[]) record, recordPath, transform);
                } else if (record instanceof Iterable) {
                    return eval((Iterable<Record>) record, recordPath, transform);
                } else if (record instanceof Map) {
                    return eval((Map<?, ?>) record, recordPath, transform);
                }
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " against " + record, e);
            }

            throw new RuntimeException("Cannot evaluate RecordPath " + recordPath + " against given argument because the argument is of type " + record.getClass() + " instead of Record");
        }

        private <T> T eval(final Map<?, ?> map, final String recordPath, final Function<Object, T> transform) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final Record record = new MapRecord(ROOT_RECORD_SCHEMA, Collections.singletonMap("root", map));
            final FieldValue parentFieldValue = new StandardFieldValue(record, PARENT_RECORD_FIELD, null);
            final FieldValue fieldValue = new StandardFieldValue(map, ROOT_RECORD_FIELD, parentFieldValue);
            final RecordPathResult result = compiled.evaluate(record, fieldValue);

            return evalResults(result.getSelectedFields(), transform, () -> "RecordPath " + recordPath + " resulted in more than one return value. The RecordPath must be further constrained.");
        }


        private <T> T eval(final Record record, final String recordPath, final Function<Object, T> transform) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);
            final RecordPathResult result = compiled.evaluate((Record) record);

            return evalResults(result.getSelectedFields(), transform,
                () -> "RecordPath " + recordPath + " evaluated against " + record + " resulted in more than one return value. The RecordPath must be further constrained.");
        }

        private <T> T eval(final Record[] records, final String recordPath, final Function<Object, T> transform) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final List<FieldValue> selectedFields = new ArrayList<>();
            for (final Record record : records) {
                final RecordPathResult result = compiled.evaluate(record);
                result.getSelectedFields().forEach(selectedFields::add);
            }

            if (selectedFields.isEmpty()) {
                return null;
            }

            return evalResults(selectedFields.stream(), transform, () -> "RecordPath " + recordPath + " resulted in more than one return value. The RecordPath must be further constrained.");
        }

        private <T> T  eval(final Iterable<Record> records, final String recordPath, final Function<Object, T> transform) {
            final RecordPath compiled = RECORD_PATH_CACHE.getCompiled(recordPath);

            final List<FieldValue> selectedFields = new ArrayList<>();
            for (final Record record : records) {
                final RecordPathResult result = compiled.evaluate(record);
                result.getSelectedFields().forEach(selectedFields::add);
            }

            if (selectedFields.isEmpty()) {
                return null;
            }

            return evalResults(selectedFields.stream(), transform, () -> "RecordPath " + recordPath + " resulted in more than one return value. The RecordPath must be further constrained.");
        }


        private <T> T evalResults(final Stream<FieldValue> fields, final Function<Object, T> transform, final Supplier<String> multipleReturnValueErrorSupplier) {
            return fields.map(FieldValue::getValue)
                .filter(Objects::nonNull)
                .map(transform)
                .reduce((a, b) -> {
                    // Only allow a single value
                    throw new RuntimeException(multipleReturnValueErrorSupplier.get());
                })
                .orElse(null);

        }
    }


}
