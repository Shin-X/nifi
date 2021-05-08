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
package org.apache.nifi.atlas.reporting;

import com.sun.jersey.api.client.Client;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.hook.AtlasHook;
import org.apache.commons.configuration.Configuration;
import org.apache.nifi.atlas.NiFiAtlasClient;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.MockPropertyValue;
import org.apache.nifi.util.MockValidationContext;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_CONF_CREATE;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_CONF_DIR;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_CONNECT_TIMEOUT;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_DEFAULT_CLUSTER_NAME;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_NIFI_URL;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_PASSWORD;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_READ_TIMEOUT;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_URLS;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.ATLAS_USER;
import static org.apache.nifi.atlas.reporting.ReportLineageToAtlas.KAFKA_BOOTSTRAP_SERVERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestReportLineageToAtlas {

    private final Logger logger = LoggerFactory.getLogger(TestReportLineageToAtlas.class);

    private ReportLineageToAtlas testSubject;
    private MockComponentLog componentLogger;
    private ReportingInitializationContext initializationContext;
    private ReportingContext reportingContext;

    @Before
    public void setUp() throws Exception {
        testSubject = new ReportLineageToAtlas();
        componentLogger = new MockComponentLog("reporting-task-id", testSubject);

        initializationContext = mock(ReportingInitializationContext.class);
        when(initializationContext.getLogger()).thenReturn(componentLogger);
    }

    @Test
    public void validateAtlasUrlsFromProperty() throws Exception {
        final MockProcessContext processContext = new MockProcessContext(testSubject);
        final MockValidationContext validationContext = new MockValidationContext(processContext);

        processContext.setProperty(ATLAS_NIFI_URL, "http://nifi.example.com:8080/nifi");
        processContext.setProperty(ATLAS_USER, "admin");
        processContext.setProperty(ATLAS_PASSWORD, "admin");

        BiConsumer<Collection<ValidationResult>, Consumer<ValidationResult>> assertResults = (rs, a) -> {
            assertTrue(rs.iterator().hasNext());
            for (ValidationResult r : rs) {
                logger.info("{}", r);
                final String subject = r.getSubject();
                if (ATLAS_URLS.getDisplayName().equals(subject)) {
                    a.accept(r);
                }
            }
        };

        // Default setting or empty urls
        assertTrue(processContext.isValid());

        processContext.removeProperty(ATLAS_URLS);
        assertTrue(processContext.isValid());

        // Invalid URL.
        processContext.setProperty(ATLAS_URLS, "invalid");
        assertResults.accept(testSubject.validate(validationContext),
                r -> assertTrue("Atlas URLs is invalid", !r.isValid()));

        // Valid URL
        processContext.setProperty(ATLAS_URLS, "http://atlas.example.com:21000");
        assertTrue(processContext.isValid());

        // Valid URL with Expression
        processContext.setProperty(ATLAS_URLS, "http://atlas.example.com:${literal(21000)}");
        assertTrue(processContext.isValid());

        // Valid URLs
        processContext.setProperty(ATLAS_URLS, "http://atlas1.example.com:21000, http://atlas2.example.com:21000");
        assertTrue(processContext.isValid());

        // Invalid and Valid URLs
        processContext.setProperty(ATLAS_URLS, "invalid, http://atlas2.example.com:21000");
        assertResults.accept(testSubject.validate(validationContext),
                r -> assertTrue("Atlas URLs is invalid", !r.isValid()));
    }

    @Test
    public void validateNoAtlasUrlsFromConfig() throws Exception {
        // GIVEN
        Properties atlasConf = new Properties();

        Consumer<Exception> assertion = e -> assertEquals(
            "No Atlas URL has been specified! Set either the 'Atlas URLs' property on the processor or the 'atlas.rest.address' property in the atlas configuration file.",
            e.getMessage()
        );

        // WHEN
        // THEN
        validateAtlasUrlsFromConfig(atlasConf, assertion);
    }

    @Test
    public void validateNoProtocolAtlasUrlsFromConfig() throws Exception {
        // GIVEN
        String atlasUrls = "noProtocolUrl, https://atlasUrl";

        Properties atlasConf = new Properties();
        atlasConf.setProperty("atlas.rest.address", atlasUrls);

        Consumer<Exception> assertion = e -> assertTrue(
            "Expected " + MalformedURLException.class.getSimpleName() + " for " + atlasUrls + ", got " + e,
            e.getCause() instanceof MalformedURLException
        );

        // WHEN
        // THEN
        validateAtlasUrlsFromConfig(atlasConf, assertion);
    }

    private void validateAtlasUrlsFromConfig(Properties atlasConf, Consumer<Exception> exceptionConsumer) throws Exception {
        // GIVEN
        Consumer<Map<PropertyDescriptor, String>> propertiesAdjustment = properties -> {
            properties.put(ATLAS_CONF_CREATE, "false");
            properties.remove(ATLAS_URLS);
        };

        // WHEN
        // THEN
        testSetup(
            atlasConf,
            propertiesAdjustment,
            () -> fail(),
            e -> {
                assertTrue("Expected a " + ProcessException.class.getSimpleName() + ", got " + e, e instanceof ProcessException);
                exceptionConsumer.accept(e);
            }
        );
    }

    @Test
    public void testCreateAtlasPropertiesWithAtlasURLs() throws Exception {
        // GIVEN
        String atlasUrls = "http://atlasUrl1,http://atlasUrl2";

        Properties atlasConf = new Properties();

        Consumer<Map<PropertyDescriptor, String>> propertiesAdjustment = properties -> {
            properties.put(ATLAS_CONF_CREATE, "true");
            properties.put(ATLAS_URLS, atlasUrls);
        };

        Runnable assertion = () -> {
            Properties atlasProperties = new Properties();
            final File atlasPropertiesFile = new File("target/atlasConfDir", "atlas-application.properties");
            try (InputStream in = new FileInputStream(atlasPropertiesFile)) {
                atlasProperties.load(in);
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            assertEquals(atlasUrls, atlasProperties.getProperty("atlas.rest.address"));
        };


        // WHEN
        // THEN
        testSetup(
            atlasConf,
            propertiesAdjustment,
            assertion,
            e -> {
                throw new AssertionError(e);
            }
        );
    }

    @Test
    public void testCreateAtlasPropertiesWithMetadataNamespace() throws Exception {
        // GIVEN
        String atlasMetadataNamespace = "namespace";

        Properties atlasConf = new Properties();

        Consumer<Map<PropertyDescriptor, String>> propertiesAdjustment = properties -> {
            properties.put(ATLAS_CONF_CREATE, "true");
            properties.put(ATLAS_DEFAULT_CLUSTER_NAME, atlasMetadataNamespace);
        };

        Runnable assertion = () -> {
            Properties atlasProperties = new Properties();
            final File atlasPropertiesFile = new File("target/atlasConfDir", "atlas-application.properties");
            try (InputStream in = new FileInputStream(atlasPropertiesFile)) {
                atlasProperties.load(in);
            } catch (Exception e) {
                throw new AssertionError(e);
            }

            assertEquals(atlasMetadataNamespace, atlasProperties.getProperty("atlas.metadata.namespace"));
        };


        // WHEN
        // THEN
        testSetup(
            atlasConf,
            propertiesAdjustment,
            assertion,
            e -> {
                throw new AssertionError(e);
            }
        );
    }

    private void testSetup(
        Properties atlasConf,
        Consumer<Map<PropertyDescriptor, String>> propertiesAdjustment,
        Runnable onSuccess, Consumer<Exception> exceptionConsumer
    ) throws Exception {
        // GIVEN
        String atlasConfDir = createAtlasConfDir();

        Map<PropertyDescriptor, String> properties = initReportingTaskProperties(atlasConfDir);
        propertiesAdjustment.accept(properties);

        saveAtlasConf(atlasConfDir, atlasConf);

        reportingContext = mock(ReportingContext.class);
        when(reportingContext.getProperties()).thenReturn(properties);
        when(reportingContext.getProperty(any())).then(invocation -> new MockPropertyValue(properties.get(invocation.getArguments()[0])));

        ConfigurationContext configurationContext = new MockConfigurationContext(properties, null);

        testSubject.initialize(initializationContext);

        // WHEN
        try {
            testSubject.setup(configurationContext);
            onSuccess.run();

            // THEN
        } catch (Exception e) {
            exceptionConsumer.accept(e);
        }
    }

    @Test
    public void testDefaultConnectAndReadTimeout() throws Exception {
        // GIVEN
        String atlasConfDir = createAtlasConfDir();

        Map<PropertyDescriptor, String> properties = initReportingTaskProperties(atlasConfDir);

        // WHEN
        // THEN
        testConnectAndReadTimeout(properties, 60000, 60000);
    }

    @Test
    public void testSetConnectAndReadTimeout() throws Exception {
        // GIVEN
        int expectedConnectTimeoutMs = 10000;
        int expectedReadTimeoutMs = 5000;

        String atlasConfDir = createAtlasConfDir();

        Map<PropertyDescriptor, String> properties = initReportingTaskProperties(atlasConfDir);
        properties.put(ATLAS_CONNECT_TIMEOUT, (expectedConnectTimeoutMs / 1000) + " sec");
        properties.put(ATLAS_READ_TIMEOUT, (expectedReadTimeoutMs / 1000) + " sec");

        // WHEN
        // THEN
        testConnectAndReadTimeout(properties, expectedConnectTimeoutMs, expectedReadTimeoutMs);
    }

    private void testConnectAndReadTimeout(Map<PropertyDescriptor, String> properties, Integer expectedConnectTimeout, Integer expectedReadTimeout) throws Exception {
        // GIVEN
        reportingContext = mock(ReportingContext.class);
        when(reportingContext.getProperties()).thenReturn(properties);
        when(reportingContext.getProperty(any())).then(invocation -> new MockPropertyValue(properties.get(invocation.getArguments()[0])));

        ConfigurationContext configurationContext = new MockConfigurationContext(properties, null);

        testSubject.initialize(initializationContext);
        testSubject.setup(configurationContext);

        // WHEN
        NiFiAtlasClient niFiAtlasClient = testSubject.createNiFiAtlasClient(reportingContext);

        // THEN
        Field fieldAtlasClient = niFiAtlasClient.getClass().getDeclaredField("atlasClient");
        fieldAtlasClient.setAccessible(true);
        AtlasClientV2 atlasClient = (AtlasClientV2) fieldAtlasClient.get(niFiAtlasClient);

        Field fieldAtlasClientContext = atlasClient.getClass().getSuperclass().getDeclaredField("atlasClientContext");
        fieldAtlasClientContext.setAccessible(true);
        Object atlasClientContext = fieldAtlasClientContext.get(atlasClient);

        Method getClient = atlasClientContext.getClass().getMethod("getClient");
        getClient.setAccessible(true);
        Client jerseyClient = (Client) getClient.invoke(atlasClientContext);
        Map<String, Object> jerseyProperties = jerseyClient.getProperties();

        Integer actualConnectTimeout = (Integer) jerseyProperties.get("com.sun.jersey.client.property.connectTimeout");
        Integer actualReadTimeout = (Integer) jerseyProperties.get("com.sun.jersey.client.property.readTimeout");

        assertEquals(expectedConnectTimeout, actualConnectTimeout);
        assertEquals(expectedReadTimeout, actualReadTimeout);
    }

    @Test
    public void testNotificationSendingIsSynchronousWhenAtlasConfIsGenerated() throws Exception {
        String atlasConfDir = createAtlasConfDir();

        Map<PropertyDescriptor, String> properties = initReportingTaskProperties(atlasConfDir);

        testNotificationSendingIsSynchronous(properties);
    }

    @Test
    public void testNotificationSendingIsSynchronousWhenAtlasConfIsProvidedAndSynchronousModeHasBeenSet() throws Exception {
        String atlasConfDir = createAtlasConfDir();

        Properties atlasConf = new Properties();
        atlasConf.setProperty(AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS, "false");
        saveAtlasConf(atlasConfDir, atlasConf);

        Map<PropertyDescriptor, String> properties = initReportingTaskProperties(atlasConfDir);
        properties.put(ATLAS_CONF_CREATE, "false");

        testNotificationSendingIsSynchronous(properties);
    }

    private void testNotificationSendingIsSynchronous(Map<PropertyDescriptor, String> properties) throws Exception {
        ConfigurationContext configurationContext = new MockConfigurationContext(properties, null);

        testSubject.initialize(initializationContext);
        testSubject.setup(configurationContext);

        Configuration atlasProperties = ApplicationProperties.get();
        boolean isAsync = atlasProperties.getBoolean(AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS, Boolean.TRUE);
        assertFalse(isAsync);
    }

    @Test(expected = ProcessException.class)
    public void testThrowExceptionWhenAtlasConfIsProvidedButSynchronousModeHasNotBeenSet() throws Exception {
        String atlasConfDir = createAtlasConfDir();

        Properties atlasConf = new Properties();
        saveAtlasConf(atlasConfDir, atlasConf);

        Map<PropertyDescriptor, String> properties = initReportingTaskProperties(atlasConfDir);
        properties.put(ATLAS_CONF_CREATE, "false");

        ConfigurationContext configurationContext = new MockConfigurationContext(properties, null);

        testSubject.initialize(initializationContext);
        testSubject.setup(configurationContext);
    }

    private String createAtlasConfDir() {
        String atlasConfDir = "target/atlasConfDir";
        File directory = new File(atlasConfDir);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        return atlasConfDir;
    }

    private void saveAtlasConf(String atlasConfDir, Properties atlasConf) throws IOException {
        FileOutputStream fos = new FileOutputStream(atlasConfDir + File.separator + ApplicationProperties.APPLICATION_PROPERTIES);
        atlasConf.store(fos, "Atlas test config");
    }

    private Map<PropertyDescriptor, String> initReportingTaskProperties(String atlasConfDir) {
        Map<PropertyDescriptor, String> properties = new HashMap<>();

        properties.put(ATLAS_URLS, "http://localhost:21000");
        properties.put(ATLAS_NIFI_URL, "http://localhost:8080/nifi");
        properties.put(ATLAS_CONF_DIR, atlasConfDir);
        properties.put(ATLAS_CONF_CREATE, "true");
        properties.put(ATLAS_DEFAULT_CLUSTER_NAME, "defaultClusterName");
        properties.put(ATLAS_USER, "admin");
        properties.put(ATLAS_PASSWORD, "password");
        properties.put(KAFKA_BOOTSTRAP_SERVERS, "http://localhost:9092");

        return properties;
    }
}
