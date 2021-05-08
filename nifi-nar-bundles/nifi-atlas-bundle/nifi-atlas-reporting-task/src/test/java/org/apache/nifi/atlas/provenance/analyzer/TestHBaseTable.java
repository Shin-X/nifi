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

import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.when;

public class TestHBaseTable {

    @Test
    public void testHBaseTable() {
        final String processorName = "FetchHBaseRow";
        final String transitUri = "hbase://0.example.com/tableA/rowB";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.FETCH);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(matches(".+\\.example\\.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals("hbase_table", ref.getTypeName());
        assertEquals("tableA", ref.get(ATTR_NAME));
        assertEquals("tableA@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

    @Test
    public void testHBaseTableWithMultipleZkHosts() {
        final String processorName = "FetchHBaseRow";
        final String transitUri = "hbase://zk0.example.com,zk2.example.com,zk3.example.com/tableA/rowB";
        final ProvenanceEventRecord record = Mockito.mock(ProvenanceEventRecord.class);
        when(record.getComponentType()).thenReturn(processorName);
        when(record.getTransitUri()).thenReturn(transitUri);
        when(record.getEventType()).thenReturn(ProvenanceEventType.FETCH);

        final NamespaceResolvers namespaceResolvers = Mockito.mock(NamespaceResolvers.class);
        when(namespaceResolvers.fromHostNames(
                matches("zk0.example.com"),
                matches("zk2.example.com"),
                matches("zk3.example.com"))).thenReturn("namespace1");

        final AnalysisContext context = Mockito.mock(AnalysisContext.class);
        when(context.getNamespaceResolver()).thenReturn(namespaceResolvers);

        final NiFiProvenanceEventAnalyzer analyzer = NiFiProvenanceEventAnalyzerFactory.getAnalyzer(processorName, transitUri, record.getEventType());
        assertNotNull(analyzer);

        final DataSetRefs refs = analyzer.analyze(context, record);
        assertEquals(1, refs.getInputs().size());
        assertEquals(0, refs.getOutputs().size());
        Referenceable ref = refs.getInputs().iterator().next();
        assertEquals("hbase_table", ref.getTypeName());
        assertEquals("tableA", ref.get(ATTR_NAME));
        assertEquals("tableA@namespace1", ref.get(ATTR_QUALIFIED_NAME));
    }

}
