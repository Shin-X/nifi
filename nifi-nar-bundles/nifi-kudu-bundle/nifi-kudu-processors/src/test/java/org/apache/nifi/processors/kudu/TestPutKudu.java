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

package org.apache.nifi.processors.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowErrorsAndOverflowStatus;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.Tuple;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.nifi.processors.kudu.TestPutKudu.ResultCode.EXCEPTION;
import static org.apache.nifi.processors.kudu.TestPutKudu.ResultCode.FAIL;
import static org.apache.nifi.processors.kudu.TestPutKudu.ResultCode.OK;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutKudu {

    public static final String DEFAULT_TABLE_NAME = "Nifi-Kudu-Table";
    public static final String DEFAULT_MASTERS = "testLocalHost:7051";
    public static final String SKIP_HEAD_LINE = "false";
    public static final String TABLE_SCHEMA = "id,stringVal,num32Val,doubleVal,decimalVal,dateVal";

    private TestRunner testRunner;

    private MockPutKudu processor;

    private MockRecordParser readerFactory;

    private final java.sql.Date today = new java.sql.Date(System.currentTimeMillis());

    @Before
    public void setUp() throws InitializationException {
        processor = new MockPutKudu();
        testRunner = TestRunners.newTestRunner(processor);
        setUpTestRunner(testRunner);
    }

    private void setUpTestRunner(TestRunner testRunner) throws InitializationException {
        testRunner.setProperty(PutKudu.TABLE_NAME, DEFAULT_TABLE_NAME);
        testRunner.setProperty(PutKudu.KUDU_MASTERS, DEFAULT_MASTERS);
        testRunner.setProperty(PutKudu.SKIP_HEAD_LINE, SKIP_HEAD_LINE);
        testRunner.setProperty(PutKudu.IGNORE_NULL, "true");
        testRunner.setProperty(PutKudu.LOWERCASE_FIELD_NAMES, "true");
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");
        testRunner.setProperty(PutKudu.INSERT_OPERATION, OperationType.INSERT.toString());
    }

    @After
    public void close() {
        testRunner = null;
    }

    private void createRecordReader(int numOfRecord) throws InitializationException {

        readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("id", RecordFieldType.INT);
        readerFactory.addSchemaField("stringVal", RecordFieldType.STRING);
        readerFactory.addSchemaField("num32Val", RecordFieldType.INT);
        readerFactory.addSchemaField("doubleVal", RecordFieldType.DOUBLE);
        readerFactory.addSchemaField(new RecordField("decimalVal", RecordFieldType.DECIMAL.getDecimalDataType(6, 3)));
        readerFactory.addSchemaField("dateVal", RecordFieldType.DATE);
        for (int i = 0; i < numOfRecord; i++) {
            readerFactory.addRecord(i, "val_" + i, 1000 + i, 100.88 + i,
                    new BigDecimal(111.111D).add(BigDecimal.valueOf(i)), today);
        }

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
    }

    @Test
    public void testCustomValidate() throws InitializationException {
        createRecordReader(1);

        testRunner.setProperty(PutKudu.KERBEROS_PRINCIPAL, "principal");
        testRunner.assertNotValid();

        testRunner.removeProperty(PutKudu.KERBEROS_PRINCIPAL);
        testRunner.setProperty(PutKudu.KERBEROS_PASSWORD, "password");
        testRunner.assertNotValid();

        testRunner.setProperty(PutKudu.KERBEROS_PRINCIPAL, "principal");
        testRunner.setProperty(PutKudu.KERBEROS_PASSWORD, "password");
        testRunner.assertValid();

        final KerberosCredentialsService kerberosCredentialsService = new MockKerberosCredentialsService("unit-test-principal", "unit-test-keytab");
        testRunner.addControllerService("kerb", kerberosCredentialsService);
        testRunner.enableControllerService(kerberosCredentialsService);
        testRunner.setProperty(PutKudu.KERBEROS_CREDENTIALS_SERVICE, "kerb");
        testRunner.assertNotValid();

        testRunner.removeProperty(PutKudu.KERBEROS_PRINCIPAL);
        testRunner.removeProperty(PutKudu.KERBEROS_PASSWORD);
        testRunner.assertValid();
    }

    @Test
    public void testWriteKuduWithDefaults() throws IOException, InitializationException {
        createRecordReader(100);

        final String filename = "testWriteKudu-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);

        // verify the successful flow file has the expected content & attributes
        final MockFlowFile mockFlowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), filename);
        mockFlowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "100");
        mockFlowFile.assertContentEquals("trigger");

        // verify we generated a provenance event
        final List<ProvenanceEventRecord> provEvents = testRunner.getProvenanceEvents();
        Assert.assertEquals(1, provEvents.size());

        // verify it was a SEND event with the correct URI
        final ProvenanceEventRecord provEvent = provEvents.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
    }

    @Test
    public void testKerberosEnabled() throws InitializationException {
        createRecordReader(1);

        final KerberosCredentialsService kerberosCredentialsService = new MockKerberosCredentialsService("unit-test-principal", "unit-test-keytab");
        testRunner.addControllerService("kerb", kerberosCredentialsService);
        testRunner.enableControllerService(kerberosCredentialsService);

        testRunner.setProperty(PutKudu.KERBEROS_CREDENTIALS_SERVICE, "kerb");

        testRunner.run(1, false);

        final MockPutKudu proc = (MockPutKudu) testRunner.getProcessor();
        assertTrue(proc.loggedIn());
        assertFalse(proc.loggedOut());

        testRunner.run(1, true, false);
        assertTrue(proc.loggedOut());
    }

    @Test
    public void testInsecureClient() throws InitializationException {
        createRecordReader(1);

        testRunner.run(1, false);

        final MockPutKudu proc = (MockPutKudu) testRunner.getProcessor();
        assertFalse(proc.loggedIn());
        assertFalse(proc.loggedOut());

        testRunner.run(1, true, false);
        assertFalse(proc.loggedOut());
    }


    @Test
    public void testInvalidReaderShouldRouteToFailure() throws InitializationException, SchemaNotFoundException, MalformedRecordException, IOException {
        createRecordReader(0);

        // simulate throwing an IOException when the factory creates a reader which is what would happen when
        // invalid Avro is passed to the Avro reader factory
        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenThrow(new IOException("NOT AVRO"));

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");

        final String filename = "testInvalidAvroShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }

    @Test
    public void testValidSchemaShouldBeSuccessful() throws InitializationException, IOException {
        createRecordReader(10);
        final String filename = "testValidSchemaShouldBeSuccessful-" + System.currentTimeMillis();

        // don't provide my.schema as an attribute
        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);
        flowFileAttributes.put("my.schema", TABLE_SCHEMA);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testAddingMissingFieldsWhenHandleSchemaDriftIsAllowed() throws InitializationException, IOException {
        // given
        processor.setTableSchema(new Schema(Arrays.asList()));
        createRecordReader(5);
        final String filename = "testAddingMissingFieldsWhenHandleSchemaDriftIsAllowed-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.setProperty(PutKudu.HANDLE_SCHEMA_DRIFT, "true");
        testRunner.enqueue("trigger", flowFileAttributes);

        // when
        testRunner.run();

        // then
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testMalformedRecordExceptionFromReaderShouldRouteToFailure() throws InitializationException, IOException, MalformedRecordException, SchemaNotFoundException {
        createRecordReader(10);

        final RecordReader recordReader = mock(RecordReader.class);
        when(recordReader.nextRecord()).thenThrow(new MalformedRecordException("ERROR"));

        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.getIdentifier()).thenReturn("mock-reader-factory");
        when(readerFactory.createRecordReader(any(FlowFile.class), any(InputStream.class), any(ComponentLog.class))).thenReturn(recordReader);

        testRunner.addControllerService("mock-reader-factory", readerFactory);
        testRunner.enableControllerService(readerFactory);
        testRunner.setProperty(PutKudu.RECORD_READER, "mock-reader-factory");

        final String filename = "testMalformedRecordExceptionShouldRouteToFailure-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }

    @Test
    public void testReadAsStringAndWriteAsInt() throws InitializationException, IOException {
        createRecordReader(0);
        // add the favorite color as a string
        readerFactory.addRecord(1, "name0", "0", "89.89", "111.111", today);

        final String filename = "testReadAsStringAndWriteAsInt-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
    }

    @Test
    public void testMissingColumInReader() throws InitializationException, IOException {
        createRecordReader(0);
        readerFactory.addRecord("name0", "0", "89.89"); //missing id

        final String filename = "testMissingColumInReader-" + System.currentTimeMillis();

        final Map<String, String> flowFileAttributes = new HashMap<>();
        flowFileAttributes.put(CoreAttributes.FILENAME.key(), filename);

        testRunner.enqueue("trigger", flowFileAttributes);
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_FAILURE, 1);
    }


    @Test
    public void testInsertManyFlowFiles() throws Exception {
        createRecordReader(50);
        final String content1 = "{ \"field1\" : \"value1\", \"field2\" : \"valu11\" }";
        final String content2 = "{ \"field1\" : \"value1\", \"field2\" : \"value11\" }";
        final String content3 = "{ \"field1\" : \"value3\", \"field2\" : \"value33\" }";

        testRunner.enqueue(content1.getBytes());
        testRunner.enqueue(content2.getBytes());
        testRunner.enqueue(content3.getBytes());

        testRunner.run(3);

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 3);
        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS);

        flowFiles.get(0).assertContentEquals(content1.getBytes());
        flowFiles.get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");

        flowFiles.get(1).assertContentEquals(content2.getBytes());
        flowFiles.get(1).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");

        flowFiles.get(2).assertContentEquals(content3.getBytes());
        flowFiles.get(2).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testUpsertFlowFiles() throws Exception {
        createRecordReader(50);
        testRunner.setProperty(PutKudu.INSERT_OPERATION, OperationType.UPSERT.toString());
        testRunner.enqueue("string".getBytes());

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);

        flowFile.assertContentEquals("string".getBytes());
        flowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testDeleteFlowFiles() throws Exception {
        createRecordReader(50);
        testRunner.setProperty(PutKudu.INSERT_OPERATION, "${kudu.record.delete}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("kudu.record.delete", "DELETE");

        testRunner.enqueue("string".getBytes(), attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);

        flowFile.assertContentEquals("string".getBytes());
        flowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testUpdateFlowFiles() throws Exception {
        createRecordReader(50);
        testRunner.setProperty(PutKudu.INSERT_OPERATION, "${kudu.record.update}");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("kudu.record.update", "UPDATE");

        testRunner.enqueue("string".getBytes(), attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutKudu.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0);

        flowFile.assertContentEquals("string".getBytes());
        flowFile.assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "50");
    }

    @Test
    public void testBuildRow() {
        buildPartialRow((long) 1, "foo", (short) 10, "id", "id", "SFO", null, false);
    }

    @Test
    public void testBuildPartialRowNullable() {
        buildPartialRow((long) 1, null, (short) 10, "id", "id", null, null, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildPartialRowNullPrimaryKey() {
        buildPartialRow(null, "foo", (short) 10, "id", "id", "SFO", null, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildPartialRowNotNullable() {
        buildPartialRow((long) 1, "foo", null, "id", "id", "SFO", null, false);
    }

    @Test
    public void testBuildPartialRowLowercaseFields() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "SFO", null, true);
        row.getLong("id");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildPartialRowLowercaseFieldsFalse() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "SFO", null, false);
        row.getLong("id");
    }

    @Test
    public void testBuildPartialRowLowercaseFieldsKuduUpper() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "ID", "ID", "SFO", null, false);
        row.getLong("ID");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildPartialRowLowercaseFieldsKuduUpperFail() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "ID", "ID", "SFO", null, true);
        row.getLong("ID");
    }

    @Test
    public void testBuildPartialRowVarCharTooLong() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "San Francisco", null, true);
        Assert.assertEquals("Kudu client should truncate VARCHAR value to expected length", "San", row.getVarchar("airport_code"));
    }

    @Test
    public void testBuildPartialRowWithDate() {
        PartialRow row = buildPartialRow((long) 1, "foo", (short) 10, "id", "ID", "San Francisco", today, true);
        // Comparing string representations of dates, because java.sql.Date does not override
        // java.util.Date.equals method and therefore compares milliseconds instead of
        // comparing dates, even though java.sql.Date is supposed to ignore time
        Assert.assertEquals(String.format("Expecting the date to be %s, but got %s", today.toString(), row.getDate("sql_date").toString()),
                row.getDate("sql_date").toString(), today.toString());
    }

    private PartialRow buildPartialRow(Long id, String name, Short age, String kuduIdName, String recordIdName, String airport_code, java.sql.Date sql_date, Boolean lowercaseFields) {
        final Schema kuduSchema = new Schema(Arrays.asList(
                new ColumnSchema.ColumnSchemaBuilder(kuduIdName, Type.INT64).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("age", Type.INT16).nullable(false).build(),
                new ColumnSchema.ColumnSchemaBuilder("updated_at", Type.UNIXTIME_MICROS).nullable(false).build(),
                new ColumnSchema.ColumnSchemaBuilder("score", Type.DECIMAL).nullable(true).typeAttributes(
                        new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(9).scale(0).build()
                ).build(),
                new ColumnSchema.ColumnSchemaBuilder("airport_code", Type.VARCHAR).nullable(true).typeAttributes(
                        new ColumnTypeAttributes.ColumnTypeAttributesBuilder().length(3).build()
                ).build(),
                new ColumnSchema.ColumnSchemaBuilder("sql_date", Type.DATE).nullable(true).build()
        ));


        final RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
                new RecordField(recordIdName, RecordFieldType.BIGINT.getDataType()),
                new RecordField("name", RecordFieldType.STRING.getDataType()),
                new RecordField("age", RecordFieldType.SHORT.getDataType()),
                new RecordField("updated_at", RecordFieldType.TIMESTAMP.getDataType()),
                new RecordField("score", RecordFieldType.LONG.getDataType()),
                new RecordField("airport_code", RecordFieldType.STRING.getDataType()),
                new RecordField("sql_date", RecordFieldType.DATE.getDataType())
        ));

        Map<String, Object> values = new HashMap<>();
        PartialRow row = kuduSchema.newPartialRow();
        values.put(recordIdName, id);
        values.put("name", name);
        values.put("age", age);
        values.put("updated_at", new Timestamp(System.currentTimeMillis()));
        values.put("score", 10000L);
        values.put("airport_code", airport_code);
        values.put("sql_date", sql_date);
        processor.buildPartialRow(
                kuduSchema,
                row,
                new MapRecord(schema, values),
                schema.getFieldNames(),
                true,
                lowercaseFields
        );
        return row;
    }

    private Tuple<Insert, OperationResponse> insert(boolean success) {
        Insert insert = mock(Insert.class);
        OperationResponse response = mock(OperationResponse.class, Mockito.RETURNS_DEEP_STUBS);
        when(response.hasRowError()).thenReturn(!success);
        if (!success) {
            when(response.getRowError().getOperation()).thenReturn(insert);
        }
        return new Tuple<>(insert, response);
    }

    enum ResultCode {
        OK,
        FAIL,
        EXCEPTION
    }

    private LinkedList<OperationResponse> queueInsert(MockPutKudu putKudu, KuduSession session, boolean sync, ResultCode... results) throws Exception {
        LinkedList<OperationResponse> responses = new LinkedList<>();
        for (ResultCode result : results) {
            boolean ok = result == OK;
            Tuple<Insert, OperationResponse> tuple = insert(ok);
            putKudu.queue(tuple.getKey());

            if (result == EXCEPTION) {
                when(session.apply(tuple.getKey())).thenThrow(mock(KuduException.class));
                // Stop processing the rest of the records on the first exception
                break;
            } else {
                responses.add(tuple.getValue());
                if (sync) {
                    when(session.apply(tuple.getKey())).thenReturn(ok ? null : tuple.getValue());

                    // In AUTO_FLUSH_SYNC mode, PutKudu immediately knows when an operation has failed.
                    // In that case, it does not process the rest of the records in the FlowFile.
                    if (result == FAIL) break;
                }
            }
        }
        return responses;
    }

    private static <T> void stubSlices(OngoingStubbing<T> stubbing, List<T> slices) {
        for (T slice : slices) {
            stubbing = stubbing.thenReturn(slice);
        }
    }

    private void testKuduPartialFailure(FlushMode flushMode, int batchSize) throws Exception {
        final int numFlowFiles = 4;
        final int numRecordsPerFlowFile = 3;
        final ResultCode[][] flowFileResults = new ResultCode[][]{
                new ResultCode[]{OK, OK, FAIL},

                // The last operation will not be submitted to Kudu if flush mode is AUTO_FLUSH_SYNC
                new ResultCode[]{OK, FAIL, OK},

                // Everything's okay
                new ResultCode[]{OK, OK, OK},

                // The last operation will not be submitted due to an exception from apply() call
                new ResultCode[]{OK, EXCEPTION, OK},
        };

        KuduSession session = mock(KuduSession.class);
        when(session.getFlushMode()).thenReturn(flushMode);
        MockPutKudu putKudu = new MockPutKudu(session);

        List<List<OperationResponse>> flowFileResponses = new ArrayList<>();
        boolean sync = flushMode == FlushMode.AUTO_FLUSH_SYNC;
        for (ResultCode[] results : flowFileResults) {
            flowFileResponses.add(queueInsert(putKudu, session, sync, results));
        }

        switch (flushMode) {
            case AUTO_FLUSH_SYNC:
                // flush() or close() returns an empty list
                when(session.close()).thenReturn(Collections.emptyList());
                break;
            case AUTO_FLUSH_BACKGROUND:
                // close() will be called for each batch of FlowFiles, however we do not check
                // the return value of it. Instead, we should check the pending errors of the session
                // as buffered operations may have already been flushed.
                when(session.close()).thenReturn(Collections.emptyList());

                List<RowErrorsAndOverflowStatus> pendingErrorResponses = new ArrayList<>();
                while (!flowFileResponses.isEmpty()) {
                    int sliceSize = Math.min(batchSize, flowFileResponses.size());
                    List<List<OperationResponse>> slice = flowFileResponses.subList(0, sliceSize);

                    RowErrorsAndOverflowStatus pendingErrorResponse = mock(RowErrorsAndOverflowStatus.class);
                    RowError[] rowErrors = slice.stream()
                            .flatMap(List::stream)
                            .filter(OperationResponse::hasRowError)
                            .map(OperationResponse::getRowError)
                            .toArray(RowError[]::new);
                    when(pendingErrorResponse.getRowErrors()).thenReturn(rowErrors);
                    pendingErrorResponses.add(pendingErrorResponse);

                    flowFileResponses = flowFileResponses.subList(sliceSize, flowFileResponses.size());
                }

                stubSlices(when(session.getPendingErrors()), pendingErrorResponses);
                break;
            case MANUAL_FLUSH:
                // close() will be called at the end of a batch, but flush() will also be called
                // whenever the mutation buffer of KuduSession becomes full. In PutKudu, we set
                // the size of the mutation buffer to match batchSize, so flush() is called only
                // when a FlowFile more than one record.
                List<List<OperationResponse>> flushes = new ArrayList<>();
                List<List<OperationResponse>> closes = new ArrayList<>();

                while (!flowFileResponses.isEmpty()) {
                    int sliceSize = Math.min(batchSize, flowFileResponses.size());
                    List<List<OperationResponse>> slice = flowFileResponses.subList(0, sliceSize);
                    flowFileResponses = flowFileResponses.subList(sliceSize, flowFileResponses.size());

                    List<OperationResponse> batch = new ArrayList<>();
                    for (OperationResponse response : slice.stream().flatMap(List::stream).collect(Collectors.toList())) {
                        if (batch.size() == batchSize) {
                            flushes.add(batch);
                            batch = new ArrayList<>();
                        }
                        batch.add(response);
                    }
                    if (flowFileResponses.isEmpty() && batch.size() == batchSize) {
                        // To handle the case where PutKudu ends the batch with flush()
                        // instead of close() due to the exception from the subsequent apply call.
                        flushes.add(batch);
                    } else if (batch.size() > 0) {
                        closes.add(batch);
                    }

                    if (!flushes.isEmpty()) stubSlices(when(session.flush()), flushes);
                    if (!closes.isEmpty()) stubSlices(when(session.close()), closes);
                }
                break;
        }

        testRunner = TestRunners.newTestRunner(putKudu);
        createRecordReader(numRecordsPerFlowFile);
        setUpTestRunner(testRunner);
        testRunner.setProperty(PutKudu.FLUSH_MODE, flushMode.name());
        testRunner.setProperty(PutKudu.BATCH_SIZE, String.valueOf(batchSize));
        testRunner.setProperty(PutKudu.FLOWFILE_BATCH_SIZE, String.valueOf(batchSize));

        IntStream.range(0, numFlowFiles).forEach(i -> testRunner.enqueue(""));
        testRunner.run(numFlowFiles);

        testRunner.assertTransferCount(PutKudu.REL_FAILURE, 3);

        List<MockFlowFile> failedFlowFiles = testRunner.getFlowFilesForRelationship(PutKudu.REL_FAILURE);
        failedFlowFiles.get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "2");
        failedFlowFiles.get(1).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, sync ? "1" : "2");
        failedFlowFiles.get(2).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "1");

        testRunner.assertTransferCount(PutKudu.REL_SUCCESS, 1);
        testRunner.getFlowFilesForRelationship(PutKudu.REL_SUCCESS).get(0).assertAttributeEquals(PutKudu.RECORD_COUNT_ATTR, "3");
    }

    private void testKuduPartialFailure(FlushMode flushMode) throws Exception {
        // Test against different batch sizes (up until the point where every record can be buffered at once)
        for (int i = 1; i <= 11; i++) {
            testKuduPartialFailure(flushMode, i);
        }
    }

    @Test
    public void testKuduPartialFailuresOnAutoFlushSync() throws Exception {
        testKuduPartialFailure(FlushMode.AUTO_FLUSH_SYNC);
    }

    @Test
    public void testKuduPartialFailuresOnAutoFlushBackground() throws Exception {
        testKuduPartialFailure(FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Test
    public void testKuduPartialFailuresOnManualFlush() throws Exception {
        testKuduPartialFailure(FlushMode.MANUAL_FLUSH);
    }

    public static class MockKerberosCredentialsService extends AbstractControllerService implements KerberosCredentialsService {
        private final String keytab;
        private final String principal;

        public MockKerberosCredentialsService(final String keytab, final String principal) {
            this.keytab = keytab;
            this.principal = principal;
        }

        @Override
        public String getKeytab() {
            return keytab;
        }

        @Override
        public String getPrincipal() {
            return principal;
        }
    }
}
