/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.nifi.processors.solr;

import com.google.gson.stream.JsonReader;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Assert;
import org.junit.Test;
import org.xmlunit.matchers.CompareMatcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestQuerySolr {
    static final String DEFAULT_SOLR_CORE = "testCollection";
    static final String SOLR_CONNECT = "http://localhost:8443/solr";

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    private SolrClient solrClient;

    public SolrClient createSolrClient() {
        try {
            // create an EmbeddedSolrServer for the processor to use
            String relPath = getClass().getProtectionDomain().getCodeSource()
                    .getLocation().getFile() + "../../target";

            solrClient = EmbeddedSolrServerFactory.create(EmbeddedSolrServerFactory.DEFAULT_SOLR_HOME,
                    DEFAULT_SOLR_CORE, relPath);

            for (int i = 0; i < 10; i++) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", "doc" + i);
                Date date = new Date();
                doc.addField("created", DATE_FORMAT.format(date));
                doc.addField("string_single", "single" + i + ".1");
                doc.addField("string_multi", "multi" + i + ".1");
                doc.addField("string_multi", "multi" + i + ".2");
                doc.addField("integer_single", i);
                doc.addField("integer_multi", 1);
                doc.addField("integer_multi", 2);
                doc.addField("integer_multi", 3);
                doc.addField("double_single", 0.5 + i);

                solrClient.add(doc);
            }
            solrClient.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }

        return solrClient;
    }

    private TestRunner createRunnerWithSolrClient(SolrClient solrClient) {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, SOLR_CONNECT);
        runner.setProperty(SolrUtils.COLLECTION, DEFAULT_SOLR_CORE);

        return runner;
    }

    private TestRunner createRunnerWithSolrCloudClient(SolrClient solrClient) {
        final TestableProcessor proc = new TestableProcessor(solrClient);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, SOLR_CONNECT);
        runner.setProperty(SolrUtils.COLLECTION, DEFAULT_SOLR_CORE);

        return runner;
    }

    private TestRunner createRunner() {
        final TestableProcessor proc = new TestableProcessor(null);

        TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_STANDARD.getValue());
        runner.setProperty(SolrUtils.SOLR_LOCATION, SOLR_CONNECT);
        runner.setProperty(SolrUtils.COLLECTION, DEFAULT_SOLR_CORE);

        return runner;
    }

    @Test
    public void testRepeatingParams() {
        TestRunner runner = createRunner();
        runner.enqueue(new byte[0]);

        runner.setProperty("facet.field.1", "123");
        runner.setProperty("facet.field.2", "other_field");
        runner.setProperty("f.123.facet.prefix", "pre");

        ProcessContext context = runner.getProcessContext();
        FlowFile flowFile = runner.getProcessSessionFactory().createSession().get();

        Map<String,String[]> solrParams = SolrUtils.getRequestParams(context, flowFile);

        String[] facet_fields = solrParams.get("facet.field");
        assertEquals(2, facet_fields.length);
        assertEquals("123", facet_fields[0]);
        assertEquals("other_field", facet_fields[1]);
        assertEquals("pre", solrParams.get("f.123.facet.prefix")[0]);
    }

    @Test
    public void testAllFacetCategories() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("facet.field", "integer_multi");
        runner.setProperty("facet.interval", "integer_single");
        runner.setProperty("facet.interval.set.1", "[4,7]");
        runner.setProperty("facet.interval.set.2", "[5,7]");
        runner.setProperty("facet.range", "created");
        runner.setProperty("facet.range.start", "NOW/MINUTE");
        runner.setProperty("facet.range.end", "NOW/MINUTE+1MINUTE");
        runner.setProperty("facet.range.gap", "+20SECOND");
        runner.setProperty("facet.query.1", "*:*");
        runner.setProperty("facet.query.2", "integer_multi:2");
        runner.setProperty("facet.query.3", "integer_multi:3");

        runner.enqueue(new ByteArrayInputStream(new byte[0]));
        runner.run();
        runner.assertTransferCount(QuerySolr.FACETS, 1);

        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0)))));
        reader.beginObject();
        while (reader.hasNext()) {
            String name = reader.nextName();
            if (name.equals("facet_queries")) {
                assertEquals(30, returnCheckSumForArrayOfJsonObjects(reader));
            } else if (name.equals("facet_fields")) {
                reader.beginObject();
                assertEquals(reader.nextName(), "integer_multi");
                assertEquals(returnCheckSumForArrayOfJsonObjects(reader), 30);
                reader.endObject();
            } else if (name.equals("facet_ranges")) {
                reader.beginObject();
                assertEquals(reader.nextName(), "created");
                assertEquals(returnCheckSumForArrayOfJsonObjects(reader), 10);
                reader.endObject();
            } else if (name.equals("facet_intervals")) {
                reader.beginObject();
                assertEquals(reader.nextName(), "integer_single");
                assertEquals(returnCheckSumForArrayOfJsonObjects(reader), 7);
                reader.endObject();
            }
        }
        reader.endObject();
        reader.close();
        solrClient.close();
    }

    private int returnCheckSumForArrayOfJsonObjects(JsonReader reader) throws IOException {
        int checkSum = 0;
        reader.beginArray();
        while (reader.hasNext()) {
            reader.beginObject();
            while (reader.hasNext()) {
                if (reader.nextName().equals("count")) {
                    checkSum += reader.nextInt();
                } else {
                    reader.skipValue();
                }
            }
            reader.endObject();
        }
        reader.endArray();
        return checkSum;
    }

    @Test
    public void testFacetTrueButNull() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.enqueue(new ByteArrayInputStream(new byte[0]));
        runner.run();

        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);

        // Check for empty nestet Objects in JSON
        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0)))));
        reader.beginObject();
        while (reader.hasNext()) {
            if (reader.nextName().equals("facet_queries")) {
                reader.beginArray();
                assertFalse(reader.hasNext());
                reader.endArray();
            } else {
                reader.beginObject();
                assertFalse(reader.hasNext());
                reader.endObject();
            }
        }
        reader.endObject();

        JsonReader reader_stats = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0)))));
        reader_stats.beginObject();
        assertEquals(reader_stats.nextName(), "stats_fields");
        reader_stats.beginObject();
        assertFalse(reader_stats.hasNext());
        reader_stats.endObject();
        reader_stats.endObject();

        reader.close();
        reader_stats.close();
        solrClient.close();
    }

    @Test
    public void testStats() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("stats", "true");
        runner.setProperty("stats.field", "integer_single");

        runner.enqueue(new ByteArrayInputStream(new byte[0]));
        runner.run();

        runner.assertTransferCount(QuerySolr.STATS, 1);
        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0)))));
        reader.beginObject();
        assertEquals(reader.nextName(), "stats_fields");
        reader.beginObject();
        assertEquals(reader.nextName(), "integer_single");
        reader.beginObject();
        while (reader.hasNext()) {
            String name = reader.nextName();
            switch (name) {
                case "min": assertEquals(reader.nextString(), "0.0"); break;
                case "max": assertEquals(reader.nextString(), "9.0"); break;
                case "count": assertEquals(reader.nextInt(), 10); break;
                case "sum": assertEquals(reader.nextString(), "45.0"); break;
                default: reader.skipValue(); break;
            }
        }
        reader.endObject();
        reader.endObject();
        reader.endObject();

        reader.close();
        solrClient.close();
    }

    @Test
    public void testRelationshipRoutings() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        // Set request handler for request failure
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "/nonexistentrequesthandler");

        // Processor has no input connection and fails
        runner.setNonLoopConnection(false);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(QuerySolr.FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.FAILURE).get(0);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION_MESSAGE);
        runner.clearTransferState();

        // Processor has an input connection and fails
        runner.setNonLoopConnection(true);
        runner.enqueue(new byte[0]);
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(QuerySolr.FAILURE, 1);

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.FAILURE).get(0);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION_MESSAGE);
        runner.clearTransferState();

        // Set request handler for successful request
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "/select");

        // Processor has no input connection and succeeds
        runner.setNonLoopConnection(false);
        runner.run(1, false);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        runner.clearTransferState();

        // Processor has an input connection and succeeds
        runner.setNonLoopConnection(true);
        runner.enqueue(new byte[0]);
        runner.run(1, true);
        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);
        runner.assertAllFlowFilesContainAttribute(QuerySolr.ATTRIBUTE_SOLR_CONNECT);

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile = runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile = runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        runner.clearTransferState();

        solrClient.close();
    }

    @Test
    public void testExpressionLanguageForProperties() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD.getValue());
        runner.setProperty(QuerySolr.SOLR_PARAM_QUERY, "${query}");
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "${handler}");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "${fields}");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "${sort}");
        runner.setProperty(QuerySolr.SOLR_PARAM_START, "${start}");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "${rows}");

        runner.enqueue(new byte[0], new HashMap<String,String>(){{
            put("query", "id:(doc0 OR doc1 OR doc2 OR doc3)");
            put("handler", "/select");
            put("fields", "id");
            put("sort", "id desc");
            put("start", "1");
            put("rows", "2");
        }});
        runner.run();
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc2</field></doc><doc><field name=\"id\">doc1</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));

        solrClient.close();
    }

    @Test
    public void testSingleFilterQuery() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");

        runner.setProperty("fq", "id:(doc2 OR doc3)");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc2</field></doc><doc><field name=\"id\">doc3</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));

        solrClient.close();
    }


    @Test
    public void testMultipleFilterQueries() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");

        runner.setProperty("fq.1", "id:(doc0 OR doc1 OR doc2 OR doc3)");
        runner.setProperty("fq.2", "id:(doc1 OR doc2 OR doc3 OR doc4)");
        runner.setProperty("fq.3", "id:(doc2 OR doc3 OR doc4 OR doc5)");

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc2</field></doc><doc><field name=\"id\">doc3</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));

        solrClient.close();
    }

    @Test
    public void testStandardResponse() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_QUERY, "id:(doc0 OR doc1)");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id desc");

        runner.setNonLoopConnection(false);
        runner.run();
        runner.assertAllFlowFilesTransferred(QuerySolr.RESULTS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);

        String expectedXml = "<docs><doc><field name=\"id\">doc1</field></doc><doc><field name=\"id\">doc0</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(flowFile))));

        solrClient.close();
    }

    @Test
    public void testPreserveOriginalContent() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_QUERY, "id:doc0");
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");

        String content = "test content 123";

        runner.enqueue(content);
        runner.run();
        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);

        String expectedXml = "<docs><doc><field name=\"id\">doc0</field></doc></docs>";
        assertThat(expectedXml, CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));
        assertEquals(content, new String(runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.ORIGINAL).get(0))));

        solrClient.close();
    }

    @Test
    public void testRetrievalOfFullResults() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "2");
        runner.setProperty(QuerySolr.AMOUNT_DOCUMENTS_TO_RETURN, QuerySolr.RETURN_ALL_RESULTS);

        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(QuerySolr.RESULTS, 5);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);
        runner.assertTransferCount(QuerySolr.STATS, 0);
        runner.assertTransferCount(QuerySolr.FACETS, 0);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(QuerySolr.RESULTS);
        Integer documentCounter = 0;
        Integer startParam = 0;

        for (MockFlowFile flowFile : flowFiles) {
            Map<String,String> attributes = flowFile.getAttributes();
            assertEquals(attributes.get(QuerySolr.ATTRIBUTE_SOLR_START), startParam.toString());
            startParam += 2;

            StringBuffer expectedXml = new StringBuffer()
                    .append("<docs><doc><field name=\"id\">doc")
                    .append(documentCounter++)
                    .append("</field></doc><doc><field name=\"id\">doc")
                    .append(documentCounter++)
                    .append("</field></doc></docs>");
            assertThat(expectedXml.toString(), CompareMatcher.isIdenticalTo(new String(runner.getContentAsByteArray(flowFile))));
        }

        solrClient.close();
    }

    @Test
    public void testRetrievalOfFullResults2() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "3");
        runner.setProperty(QuerySolr.AMOUNT_DOCUMENTS_TO_RETURN, QuerySolr.RETURN_ALL_RESULTS);
        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.enqueue(new byte[0]);
        runner.run();

        runner.assertTransferCount(QuerySolr.RESULTS, 4);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);

        solrClient.close();
    }

    @Test
    public void testRetrievalOfFullResults3() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id");
        runner.setProperty(QuerySolr.SOLR_PARAM_SORT, "id asc");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "3");
        runner.setProperty(QuerySolr.AMOUNT_DOCUMENTS_TO_RETURN, QuerySolr.RETURN_ALL_RESULTS);
        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.setNonLoopConnection(false);
        runner.run();

        runner.assertTransferCount(QuerySolr.RESULTS, 4);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 0);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);

        solrClient.close();
    }


    @Test
    public void testRecordResponse() throws IOException, InitializationException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.RETURN_TYPE, QuerySolr.MODE_REC.getValue());
        runner.setProperty(QuerySolr.SOLR_PARAM_FIELD_LIST, "id,created,integer_single");
        runner.setProperty(QuerySolr.SOLR_PARAM_ROWS, "10");

        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/test-schema.avsc")));

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(SolrUtils.RECORD_WRITER, "writer");

        runner.setNonLoopConnection(false);

        runner.run(1);
        runner.assertQueueEmpty();
        runner.assertTransferCount(QuerySolr.RESULTS, 1);

        JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(
                runner.getContentAsByteArray(runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0)))));
        reader.beginArray();
        int controlScore = 0;
        while (reader.hasNext()) {
            reader.beginObject();
            while (reader.hasNext()) {
                if (reader.nextName().equals("integer_single")) {
                    controlScore += reader.nextInt();
                } else {
                    reader.skipValue();
                }
            }
            reader.endObject();
        }
        reader.close();
        solrClient.close();

        assertEquals(controlScore, 45);
    }

    @Test
    public void testExceedStartParam() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrClient(solrClient);

        runner.setProperty(QuerySolr.SOLR_PARAM_START, "10001");

        runner.setNonLoopConnection(false);

        runner.run();
        runner.assertAllFlowFilesTransferred(QuerySolr.RESULTS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);

        assertEquals("10001", flowFile.getAttribute(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals(0, runner.getContentAsByteArray(flowFile).length);

        solrClient.close();
    }

    @Test
    public void testAttributesFailure() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrCloudClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");
        runner.setProperty(QuerySolr.SOLR_PARAM_REQUEST_HANDLER, "/nonexistentrequesthandler");

        runner.enqueue("");
        runner.run();

        runner.assertAllFlowFilesTransferred(QuerySolr.FAILURE, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.FAILURE).get(0);

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION);
        flowFile.assertAttributeExists(QuerySolr.EXCEPTION_MESSAGE);
    }

    @Test
    public void testAttributes() throws IOException {
        SolrClient solrClient = createSolrClient();
        TestRunner runner = createRunnerWithSolrCloudClient(solrClient);

        runner.setProperty("facet", "true");
        runner.setProperty("stats", "true");

        runner.enqueue("");
        runner.run();

        runner.assertTransferCount(QuerySolr.RESULTS, 1);
        runner.assertTransferCount(QuerySolr.FACETS, 1);
        runner.assertTransferCount(QuerySolr.STATS, 1);
        runner.assertTransferCount(QuerySolr.ORIGINAL, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(QuerySolr.RESULTS).get(0);
        Map<String, String> attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_START);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_ROWS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));

        assertEquals("q=*:*&qt=/select&start=0&rows=10&stats=true&facet=true", attributes.get(QuerySolr.ATTRIBUTE_SOLR_QUERY));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_STATUS));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_ROWS));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS));
        assertEquals(QuerySolr.MIME_TYPE_XML, attributes.get(CoreAttributes.MIME_TYPE.key()));

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.FACETS).get(0);
        attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_START);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_ROWS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));

        assertEquals("q=*:*&qt=/select&start=0&rows=10&stats=true&facet=true", attributes.get(QuerySolr.ATTRIBUTE_SOLR_QUERY));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_STATUS));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_ROWS));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS));
        assertEquals(QuerySolr.MIME_TYPE_JSON, attributes.get(CoreAttributes.MIME_TYPE.key()));

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.STATS).get(0);
        attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_CURSOR_MARK);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_STATUS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_START);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_ROWS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_QUERY_TIME);
        flowFile.assertAttributeExists(CoreAttributes.MIME_TYPE.key());

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));

        assertEquals("q=*:*&qt=/select&start=0&rows=10&stats=true&facet=true", attributes.get(QuerySolr.ATTRIBUTE_SOLR_QUERY));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_STATUS));
        assertEquals("0", attributes.get(QuerySolr.ATTRIBUTE_SOLR_START));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_ROWS));
        assertEquals("10", attributes.get(QuerySolr.ATTRIBUTE_SOLR_NUMBER_RESULTS));
        assertEquals(QuerySolr.MIME_TYPE_JSON, attributes.get(CoreAttributes.MIME_TYPE.key()));

        flowFile = runner.getFlowFilesForRelationship(QuerySolr.ORIGINAL).get(0);
        attributes = flowFile.getAttributes();

        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_CONNECT);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_COLLECTION);
        flowFile.assertAttributeExists(QuerySolr.ATTRIBUTE_SOLR_QUERY);

        assertEquals(SOLR_CONNECT, attributes.get(QuerySolr.ATTRIBUTE_SOLR_CONNECT));
        assertEquals(DEFAULT_SOLR_CORE, attributes.get(QuerySolr.ATTRIBUTE_SOLR_COLLECTION));

        solrClient.close();
    }

    // Override createSolrClient and return the passed in SolrClient
    private class TestableProcessor extends QuerySolr {
        private SolrClient solrClient;

        public TestableProcessor(SolrClient solrClient) {
            this.solrClient = solrClient;
        }
        @Override
        protected SolrClient createSolrClient(ProcessContext context, String solrLocation) {
            return solrClient;
        }
    }
}
