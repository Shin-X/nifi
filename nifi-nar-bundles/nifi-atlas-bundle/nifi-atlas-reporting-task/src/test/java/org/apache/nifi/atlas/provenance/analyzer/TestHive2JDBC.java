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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzer;
import org.apache.nifi.atlas.provenance.NiFiProvenanceEventAnalyzerFactory;
import org.apache.nifi.atlas.resolver.NamespaceResolvers;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_INPUT_TABLES;
import static org.apache.nifi.atlas.provenance.analyzer.DatabaseAnalyzerUtil.ATTR_OUTPUT_TABLES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

public class TestHive2JDBC {

    /**
     * If a provenance event does not have table name attributes,
     * then a database lineage should be created.
     */
    @Test
    public void testDatabaseLineage() {
        final String processorName = "PutHiveQL";
        final String transitUri = "jdbc:hive2://0.example.com:10000/database_A";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(0, refs.getInputs().size());
        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hive_db", ref.getTypeName());
        assertEquals("database_a", ref.get(ATTR_NAME));
        assertEquals("database_a@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

    /**
     * If a provenance event has table name attributes,
     * then table lineages can be created.
     */
    @Test
    public void testTableLineage() {
        final String processorName = "PutHiveQL";
        final String transitUri = "jdbc:hive2://0.example.com:10000/database_A";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);
        // E.g. insert into databaseB.tableB1 select something from tableA1 a1 inner join tableA2 a2 where a1.id = a2.id
        when(record.getAttribute(ATTR_INPUT_TABLES)).thenReturn("table_A1, table_A2");
        when(record.getAttribute(ATTR_OUTPUT_TABLES)).thenReturn("database_B.table_B1");

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(2, refs.getInputs().size());
        // QualifiedName : Name
        final Map<String, String> expectedInputRefs = new HashMap<>();
        expectedInputRefs.put("database_a.table_a1@namespace1", "table_a1");
        expectedInputRefs.put("database_a.table_a2@namespace1", "table_a2");
        for (Referenceable ref : refs.getInputs()) {
            final String qName = (String) ref.get(ATTR_QUALIFIED_NAME);
            assertTrue(expectedInputRefs.containsKey(qName));
            assertEquals(expectedInputRefs.get(qName), ref.get(ATTR_NAME));
        }

        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hive_table", ref.getTypeName());
        assertEquals("table_b1", ref.get(ATTR_NAME));
        assertEquals("database_b.table_b1@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

    /**
     * If a provenance event has table name attributes, then table lineages can be created.
     * In this case, if its transit URI does not contain database name, use 'default'.
     */
    @Test
    public void testTableLineageWithDefaultTableName() {
        final String processorName = "PutHiveQL";
        final String transitUri = "jdbc:hive2://0.example.com:10000";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);
        // E.g. insert into databaseB.tableB1 select something from tableA1 a1 inner join tableA2 a2 where a1.id = a2.id
        when(record.getAttribute(ATTR_INPUT_TABLES)).thenReturn("table_A1, table_A2");
        when(record.getAttribute(ATTR_OUTPUT_TABLES)).thenReturn("database_B.table_B1");

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(2, refs.getInputs().size());
        // QualifiedName : Name
        final Map<String, String> expectedInputRefs = new HashMap<>();
        expectedInputRefs.put("default.table_a1@namespace1", "table_a1");
        expectedInputRefs.put("default.table_a2@namespace1", "table_a2");
        for (Referenceable ref : refs.getInputs()) {
            final String qName = (String) ref.get(ATTR_QUALIFIED_NAME);
            assertTrue(expectedInputRefs.containsKey(qName));
            assertEquals(expectedInputRefs.get(qName), ref.get(ATTR_NAME));
        }

        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hive_table", ref.getTypeName());
        assertEquals("table_b1", ref.get(ATTR_NAME));
        assertEquals("database_b.table_b1@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

    /**
     * A Hive connection URL can have connection strings delimited by semicolons.
     */
    @Test
    public void testTableLineageWithDefaultTableNameWithConnectionParams() {
        final String processorName = "PutHiveQL";
        final String transitUri = "jdbc:hive2://0.example.com:10000;transportMode=http;httpPath=cliservice";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);
        // E.g. insert into databaseB.tableB1 select something from tableA1 a1 inner join tableA2 a2 where a1.id = a2.id
        when(record.getAttribute(ATTR_INPUT_TABLES)).thenReturn("table_A1, table_A2");
        when(record.getAttribute(ATTR_OUTPUT_TABLES)).thenReturn("database_B.table_B1");

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(2, refs.getInputs().size());
        // QualifiedName : Name
        final Map<String, String> expectedInputRefs = new HashMap<>();
        expectedInputRefs.put("default.table_a1@namespace1", "table_a1");
        expectedInputRefs.put("default.table_a2@namespace1", "table_a2");
        for (Referenceable ref : refs.getInputs()) {
            final String qName = (String) ref.get(ATTR_QUALIFIED_NAME);
            assertTrue(expectedInputRefs.containsKey(qName));
            assertEquals(expectedInputRefs.get(qName), ref.get(ATTR_NAME));
        }

        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hive_table", ref.getTypeName());
        assertEquals("table_b1", ref.get(ATTR_NAME));
        assertEquals("database_b.table_b1@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

    /**
     * Hive connection URL can have multiple zookeeper host ports
     * and multiple parameters delimited with semicolons.
     * Database name can be omitted.
     */
    @Test
    public void testTableLineageWithZookeeperDiscovery() {
        final String processorName = "PutHiveQL";
        final String transitUri = "jdbc:hive2://0.example.com:2181,1.example.com:2181,2.example.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);
        // E.g. insert into databaseB.tableB1 select something from tableA1 a1 inner join tableA2 a2 where a1.id = a2.id
        when(record.getAttribute(ATTR_INPUT_TABLES)).thenReturn("table_A1, table_A2");
        when(record.getAttribute(ATTR_OUTPUT_TABLES)).thenReturn("database_B.table_B1");

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(eq("0.example.com"), eq("1.example.com"), eq("2.example.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(2, refs.getInputs().size());
        // QualifiedName : Name
        final Map<String, String> expectedInputRefs = new HashMap<>();
        expectedInputRefs.put("default.table_a1@namespace1", "table_a1");
        expectedInputRefs.put("default.table_a2@namespace1", "table_a2");
        for (Referenceable ref : refs.getInputs()) {
            final String qName = (String) ref.get(ATTR_QUALIFIED_NAME);
            assertTrue(expectedInputRefs.containsKey(qName));
            assertEquals(expectedInputRefs.get(qName), ref.get(ATTR_NAME));
        }

        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hive_table", ref.getTypeName());
        assertEquals("table_b1", ref.get(ATTR_NAME));
        assertEquals("database_b.table_b1@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

    /**
     * Hive connection URL using zookeeper and database name.
     */
    @Test
    public void testTableLineageWithZookeeperDiscoverySpecificDatabase() {
        final String processorName = "PutHiveQL";
        final String transitUri = "jdbc:hive2://0.example.com:2181,1.example.com:2181/some_database;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.SEND);
        // E.g. insert into databaseB.tableB1 select something from tableA1 a1 inner join tableA2 a2 where a1.id = a2.id
        when(record.getAttribute(ATTR_INPUT_TABLES)).thenReturn("table_A1, table_A2");
        when(record.getAttribute(ATTR_OUTPUT_TABLES)).thenReturn("database_B.table_B1");

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(eq("0.example.com"), eq("1.example.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(2, refs.getInputs().size());
        // QualifiedName : Name
        final Map<String, String> expectedInputRefs = new HashMap<>();
        expectedInputRefs.put("some_database.table_a1@namespace1", "table_a1");
        expectedInputRefs.put("some_database.table_a2@namespace1", "table_a2");
        for (Referenceable ref : refs.getInputs()) {
            final String qName = (String) ref.get(ATTR_QUALIFIED_NAME);
            assertTrue(expectedInputRefs.containsKey(qName));
            assertEquals(expectedInputRefs.get(qName), ref.get(ATTR_NAME));
        }

        assertEquals(1, refs.getOutputs().size());
        Referenceable ref = refs.getOutputs().iterator().next();
        assertEquals("hive_table", ref.getTypeName());
        assertEquals("table_b1", ref.get(ATTR_NAME));
        assertEquals("database_b.table_b1@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

}
