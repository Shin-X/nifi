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
package org.apache.nifi.processors;

import com.maxmind.geoip2.model.CityResponse;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.maxmind.DatabaseReader;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.GeoEnrichTestUtils.getFullCityResponse;
import static org.apache.nifi.processors.GeoEnrichTestUtils.getNullLatAndLongCityResponse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({GeoEnrichIP.class})
@SuppressWarnings("WeakerAccess")
public class TestGeoEnrichIP {
    DatabaseReader databaseReader;
    GeoEnrichIP geoEnrichIP;
    TestRunner testRunner;

    @Before
    public void setUp() throws Exception {
        mockStatic(InetAddress.class);
        databaseReader = mock(DatabaseReader.class);
        geoEnrichIP = new TestableGeoEnrichIP();
        testRunner = TestRunners.newTestRunner(geoEnrichIP);
    }

    @Test
    public void verifyNonExistentIpFlowsToNotFoundRelationship() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        testRunner.enqueue(new byte[0], Collections.emptyMap());

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());

        verify(databaseReader).city(InetAddress.getByName(null));
    }

    @Test
    public void successfulMaxMindResponseShouldFlowToFoundRelationship() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        final CityResponse cityResponse = getFullCityResponse();

        when(databaseReader.city(InetAddress.getByName("1.2.3.4"))).thenReturn(cityResponse);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(0, notFound.size());
        assertEquals(1, found.size());

        FlowFile finishedFound = found.get(0);
        assertNotNull(finishedFound.getAttribute("ip.geo.lookup.micros"));
        assertEquals("Minneapolis", finishedFound.getAttribute("ip.geo.city"));
        assertEquals("44.98", finishedFound.getAttribute("ip.geo.latitude"));
        assertEquals("93.2636", finishedFound.getAttribute("ip.geo.longitude"));
        assertEquals("Minnesota", finishedFound.getAttribute("ip.geo.subdivision.0"));
        assertEquals("MN", finishedFound.getAttribute("ip.geo.subdivision.isocode.0"));
        assertNull(finishedFound.getAttribute("ip.geo.subdivision.1"));
        assertEquals("TT", finishedFound.getAttribute("ip.geo.subdivision.isocode.1"));
        assertEquals("United States of America", finishedFound.getAttribute("ip.geo.country"));
        assertEquals("US", finishedFound.getAttribute("ip.geo.country.isocode"));
        assertEquals("55401", finishedFound.getAttribute("ip.geo.postalcode"));
    }

    @Test
    public void successfulMaxMindResponseShouldFlowToFoundRelationshipWhenLatAndLongAreNotSet() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        final CityResponse cityResponse = getNullLatAndLongCityResponse();

        when(databaseReader.city(InetAddress.getByName("1.2.3.4"))).thenReturn(cityResponse);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(0, notFound.size());
        assertEquals(1, found.size());

        FlowFile finishedFound = found.get(0);
        assertNotNull(finishedFound.getAttribute("ip.geo.lookup.micros"));
        assertEquals("Minneapolis", finishedFound.getAttribute("ip.geo.city"));
        assertNull(finishedFound.getAttribute("ip.geo.latitude"));
        assertNull(finishedFound.getAttribute("ip.geo.longitude"));
        assertEquals("Minnesota", finishedFound.getAttribute("ip.geo.subdivision.0"));
        assertEquals("MN", finishedFound.getAttribute("ip.geo.subdivision.isocode.0"));
        assertNull(finishedFound.getAttribute("ip.geo.subdivision.1"));
        assertEquals("TT", finishedFound.getAttribute("ip.geo.subdivision.isocode.1"));
        assertEquals("United States of America", finishedFound.getAttribute("ip.geo.country"));
        assertEquals("US", finishedFound.getAttribute("ip.geo.country.isocode"));
        assertEquals("55401", finishedFound.getAttribute("ip.geo.postalcode"));
    }

    @Test
    public void evaluatingExpressionLanguageShouldAndFindingIpFieldWithSuccessfulLookUpShouldFlowToFoundRelationship() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "${ip.fields:substringBefore(',')}");

        final CityResponse cityResponse = getNullLatAndLongCityResponse();

        when(databaseReader.city(InetAddress.getByName("1.2.3.4"))).thenReturn(cityResponse);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip.fields", "ip0,ip1,ip2");
        attributes.put("ip0", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(0, notFound.size());
        assertEquals(1, found.size());

        FlowFile finishedFound = found.get(0);
        assertNotNull(finishedFound.getAttribute("ip0.geo.lookup.micros"));
        assertEquals("Minneapolis", finishedFound.getAttribute("ip0.geo.city"));
        assertNull(finishedFound.getAttribute("ip0.geo.latitude"));
        assertNull(finishedFound.getAttribute("ip0.geo.longitude"));
        assertEquals("Minnesota", finishedFound.getAttribute("ip0.geo.subdivision.0"));
        assertEquals("MN", finishedFound.getAttribute("ip0.geo.subdivision.isocode.0"));
        assertNull(finishedFound.getAttribute("ip0.geo.subdivision.1"));
        assertEquals("TT", finishedFound.getAttribute("ip0.geo.subdivision.isocode.1"));
        assertEquals("United States of America", finishedFound.getAttribute("ip0.geo.country"));
        assertEquals("US", finishedFound.getAttribute("ip0.geo.country.isocode"));
        assertEquals("55401", finishedFound.getAttribute("ip0.geo.postalcode"));
    }

    @Test
    public void shouldFlowToNotFoundWhenNullResponseFromMaxMind() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        when(databaseReader.city(InetAddress.getByName("1.2.3.4"))).thenReturn(null);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFlowToNotFoundWhenIOExceptionThrownFromMaxMind() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        when(databaseReader.city(InetAddress.getByName("1.2.3.4"))).thenThrow(IOException.class);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldFlowToNotFoundWhenExceptionThrownFromMaxMind() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        when(databaseReader.city(InetAddress.getByName("1.2.3.4"))).thenThrow(IOException.class);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "1.2.3.4");

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void whenInetAddressThrowsUnknownHostFlowFileShouldBeSentToNotFound() throws Exception {
        testRunner.setProperty(GeoEnrichIP.GEO_DATABASE_FILE, "./");
        testRunner.setProperty(GeoEnrichIP.IP_ADDRESS_ATTRIBUTE, "ip");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("ip", "somenonexistentdomain.comm");

        when(InetAddress.getByName("somenonexistentdomain.comm")).thenThrow(UnknownHostException.class);

        testRunner.enqueue(new byte[0], attributes);

        testRunner.run();

        List<MockFlowFile> notFound = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_NOT_FOUND);
        List<MockFlowFile> found = testRunner.getFlowFilesForRelationship(GeoEnrichIP.REL_FOUND);

        assertEquals(1, notFound.size());
        assertEquals(0, found.size());

        verify(databaseReader).close();
        verifyNoMoreInteractions(databaseReader);
    }

    class TestableGeoEnrichIP extends GeoEnrichIP {
        @OnScheduled
        @Override
        public void onScheduled(ProcessContext context) throws IOException {
            databaseReaderRef.set(databaseReader);
        }
    }
}
