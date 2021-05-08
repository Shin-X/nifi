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

package org.apache.nifi.stateless.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;
import org.apache.nifi.security.util.OkHttpClientUtils;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.stateless.core.RegistryUtil;
import org.apache.nifi.stateless.engine.StatelessEngineConfiguration;
import org.apache.nifi.stateless.flow.DataflowDefinition;
import org.apache.nifi.stateless.flow.DataflowDefinitionParser;
import org.apache.nifi.stateless.flow.StandardDataflowDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertiesFileFlowDefinitionParser implements DataflowDefinitionParser {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesFileFlowDefinitionParser.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Pattern PROPERTY_LINE_PATTERN = Pattern.compile("(.*?)(?<!\\\\)=(.*)");
    // parameter context pattern starts with "nifi.stateless.parameters." followed by the name of a parameter context.
    // After the name of the parameter context, it may or may not have a ".<parameter name>" component, then an equals (=) and a value.
    private static final Pattern PARAMETER_CONTEXT_PATTERN = Pattern.compile("\\Qnifi.stateless.parameters.\\E(.*?)(\\..*)?");
    private static final Pattern REPORTING_TASK_PATTERN = Pattern.compile("\\Qnifi.stateless.reporting.task.\\E(.*?)\\.(.*)");

    // Property names/keys
    private static final String FAILURE_PORTS_KEY = "nifi.stateless.failure.port.names";
    private static final String REGISTRY_URL_KEY = "nifi.stateless.registry.url";
    private static final String BUCKET_ID_KEY = "nifi.stateless.flow.bucketId";
    private static final String FLOW_ID_KEY = "nifi.stateless.flow.id";
    private static final String FLOW_VERSION_KEY = "nifi.stateless.flow.version";
    private static final String FLOW_SNAPSHOT_FILE_KEY = "nifi.stateless.flow.snapshot.file";
    private static final String FLOW_SNAPSHOT_URL_KEY = "nifi.stateless.flow.snapshot.url";
    private static final String FLOW_SNAPSHOT_CONTENTS_KEY = "nifi.stateless.flow.snapshot.contents";
    private static final String FLOW_SNAPSHOT_URL_USE_SSLCONTEXT_KEY = "nifi.stateless.flow.snapshot.url.use.ssl.context";
    private static final String FLOW_NAME = "nifi.stateless.flow.name";


    public DataflowDefinition<VersionedFlowSnapshot> parseFlowDefinition(final File propertiesFile, final StatelessEngineConfiguration engineConfig)
                        throws IOException, StatelessConfigurationException {
        final Map<String, String> properties = readPropertyValues(propertiesFile);
        return parseFlowDefinition(properties, engineConfig);
    }

    public DataflowDefinition<VersionedFlowSnapshot> parseFlowDefinition(final Map<String, String> properties, final StatelessEngineConfiguration engineConfig)
                        throws IOException, StatelessConfigurationException {

        // A common problem is users accidentally including whitespace at the beginning or end of property values.
        // We can't just blindly trim the white space because it may be relevant. For example, there may be a Parameter
        // that has a value of a literal space ( ), such as a character for a delimiter, and we don't want to remove
        // that and result in an empty string. So we will log a warning that it may be a problem.
        warnOnWhitespace(properties);

        final Set<String> failurePortNames = getFailurePortNames(properties);
        final VersionedFlowSnapshot flowSnapshot = fetchVersionedFlowSnapshot(properties, engineConfig.getSslContext());
        final List<ParameterContextDefinition> parameterContextDefinitions = getParameterContexts(properties);
        final List<ReportingTaskDefinition> reportingTaskDefinitions = getReportingTasks(properties);

        final String rootGroupName = flowSnapshot.getFlowContents().getName();
        final String flowName = properties.getOrDefault(FLOW_NAME, rootGroupName);

        return new StandardDataflowDefinition.Builder()
            .flowSnapshot(flowSnapshot)
            .flowName(flowName)
            .failurePortNames(failurePortNames)
            .parameterContexts(parameterContextDefinitions)
            .reportingTasks(reportingTaskDefinitions)
            .build();
    }

    private List<ReportingTaskDefinition> getReportingTasks(final Map<String, String> properties) {
        final Map<String, ReportingTaskDefinition> reportingTaskDefinitions = new LinkedHashMap<>();

        for (final String propertyName : properties.keySet()) {
            final Matcher matcher = REPORTING_TASK_PATTERN.matcher(propertyName);
            if (!matcher.matches()) {
                continue;
            }

            // For a property name like:
            // nifi.stateless.reporting.task.abc.name=hello
            // We consider 'abc' the <reporting task key> and 'name' the <relative property name>
            final String reportingTaskKey = matcher.group(1);
            final ReportingTaskDefinition definition = reportingTaskDefinitions.computeIfAbsent(reportingTaskKey, key -> new ReportingTaskDefinition());
            final String relativePropertyName = matcher.group(2);
            final String propertyValue = properties.get(propertyName);

            if (relativePropertyName.startsWith("properties.")) {
                if (relativePropertyName.length() < 12) {
                    logger.warn("Encountered unexpected property <" + propertyName + "> in flow definition. This property will be ignored.");
                    continue;
                }

                final String reportingTaskPropertyName = relativePropertyName.substring(11);
                definition.getPropertyValues().put(reportingTaskPropertyName, propertyValue);
            } else {
                switch (relativePropertyName) {
                    case "name":
                        definition.setName(propertyValue);
                        break;
                    case "type":
                        definition.setType(propertyValue);
                        break;
                    case "frequency":
                        definition.setSchedulingFrequency(propertyValue);
                        break;
                    case "bundle":
                        definition.setBundleCoordinates(propertyValue);
                        break;
                    default:
                        logger.warn("Encountered unexpected property <" + propertyName + "> in flow definition. This property will be ignored.");
                        break;
                }
            }
        }

        return new ArrayList<>(reportingTaskDefinitions.values());
    }

    private List<ParameterContextDefinition> getParameterContexts(final Map<String, String> properties) {
        final Map<String, ParameterContextDefinition> contextDefinitions = new LinkedHashMap<>();

        for (final String propertyName : properties.keySet()) {
            final Matcher matcher = PARAMETER_CONTEXT_PATTERN.matcher(propertyName);
            if (!matcher.matches()) {
                continue;
            }

            final String parameterContextKey = matcher.group(1);
            final ParameterContextDefinition paramContext = contextDefinitions.computeIfAbsent(parameterContextKey, key -> new ParameterContextDefinition());

            String parameterName = matcher.group(2);
            final String value = properties.get(propertyName);

            if (parameterName == null) {
                // If no parameter name is given, then the value is the name of the Parameter context itself.
                paramContext.setName(value);
                continue;
            }

            // If properties file contains a parameter with no name, just ignore it. However, the parameter name currently has
            // a . at the beginning because capturing group 2 captures .<parameter name> so remove the .
            if (parameterName.equals(".")) {
                continue;
            } else {
                parameterName = parameterName.substring(1);
            }

            List<ParameterDefinition> parameterDefinitions = paramContext.getParameters();
            if (parameterDefinitions == null) {
                parameterDefinitions = new ArrayList<>();
            }

            final ParameterDefinition parameter = new ParameterDefinition();
            parameter.setName(parameterName);
            parameter.setValue(value);
            parameterDefinitions.add(parameter);

            paramContext.setParameters(parameterDefinitions);
        }

        return new ArrayList<>(contextDefinitions.values());
    }

    private Set<String> getFailurePortNames(final Map<String, String> properties) {
        final Set<String> failurePortNames = new HashSet<>();
        for (final String portName : properties.getOrDefault(FAILURE_PORTS_KEY, "").split(",")) {
            failurePortNames.add(portName.trim());
        }

        return failurePortNames;
    }

    private void warnOnWhitespace(final Map<String, String> properties) {
        properties.forEach((key, value) -> {
            if (value == null) {
                return;
            }

            if (!key.trim().equals(key)) {
                logger.warn("Found property with name <{}>. This property name contains leading or trailing white space, which may not be intended.", key);
            }

            if (!value.trim().equals(value) && !value.trim().isEmpty()) {
                // If value consists only of white space, don't worry about it. But if the value consists of non-white-space characters but has leading or trailing white space
                // it may be cause for concern.
                logger.warn("Found property with name <{}> and value <{}>. This property value contains leading or trailing white space, which may not be intended.", key, value);
            }
        });
    }

    /**
     * Read the given properties file into a Map&lt;String, String&gt;. We are parsing this ourselves, rather than using Properties.load()
     * because Properties.load() does not allow for property names to contain spaces unless they are espcaed using a backslash (\).
     * This makes sense for many use cases, but for our use case, the a very common use case will be for the properties file to contain Parameters
     * whose names have spaces. Expecting users to manually escape that will be very frustrating and error-prone.
     *
     * Another option would be to avoid using Properties and instead use JSON, YAML, or the like. However, these have their downsides, as well.
     * JSON is easy to read but can be frustrating to write correctly when writing it manually. YAML will likely be easier, but it is less well known
     * than either properties or JSON.
     *
     * The final consideration is that we want to remain consistent with the configuration format. We don't want to take in a properties file for the
     * engine configuration and a JSON/YAML/XML/etc. configuration for the flow configuration. And since NiFi is already based on properties file and
     * that seems to be the easiest to manually read/write, that's what we are going with here.
     *
     * @param propertiesFile the properties file to parse
     * @return a Map of Property Name to Property Values.
     * @throws IOException if unable to read from the file.
     */
    private Map<String, String> readPropertyValues(final File propertiesFile) throws IOException {
        final Map<String, String> properties = new LinkedHashMap<>();

        try (final InputStream in = new FileInputStream(propertiesFile);
             final Reader inReader = new InputStreamReader(in);
             final BufferedReader reader = new BufferedReader(inReader)) {

            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (trimmed.startsWith("#") || trimmed.startsWith("!")) {
                    // Line is a comment
                    continue;
                }

                if (trimmed.isEmpty()) {
                    // Empty line.
                    continue;
                }

                final Matcher matcher = PROPERTY_LINE_PATTERN.matcher(line);
                if (!matcher.matches()) {
                    // Invalid line.
                    logger.warn("Encountered line in properties file {} that is not a comment, not blank, and does not adhere to the format of <propertyName>=<propertyValue>: {}",
                        propertiesFile.getAbsolutePath(), line);
                    continue;
                }

                final String propertyName = matcher.group(1);
                final String propertyValue = matcher.group(2);
                properties.put(propertyName, propertyValue);
            }
        }

        return properties;
    }

    private VersionedFlowSnapshot fetchVersionedFlowSnapshot(final Map<String, String> properties, final SslContextDefinition sslContextDefinition)
        throws IOException, StatelessConfigurationException {

        final String flowSnapshotFilename = properties.get(FLOW_SNAPSHOT_FILE_KEY);
        if (flowSnapshotFilename != null && !flowSnapshotFilename.trim().isEmpty()) {
            final File flowSnapshotFile = new File(flowSnapshotFilename.trim());
            try {
                return readVersionedFlowSnapshot(flowSnapshotFile);
            } catch (final Exception e) {
                throw new IOException("Configuration indicates that the flow to run is located at " + flowSnapshotFilename
                    + " but failed to load dataflow from that location", e);
            }
        }

        final String flowSnapshotUrl = properties.get(FLOW_SNAPSHOT_URL_KEY);
        if (flowSnapshotUrl != null && !flowSnapshotUrl.trim().isEmpty()) {
            final String useSslPropertyValue = properties.get(FLOW_SNAPSHOT_URL_USE_SSLCONTEXT_KEY);
            final boolean useSsl = Boolean.parseBoolean(useSslPropertyValue);

            try {
                return fetchFlowFromUrl(flowSnapshotUrl, useSsl ? sslContextDefinition : null);
            } catch (final Exception e) {
                throw new StatelessConfigurationException("Could not fetch flow from URL", e);
            }
        }

        final String flowContents = properties.get(FLOW_SNAPSHOT_CONTENTS_KEY);
        if (flowContents != null && !flowContents.trim().isEmpty()) {
            final byte[] flowContentsBytes = flowContents.getBytes(StandardCharsets.UTF_8);

            try (final InputStream in = new ByteArrayInputStream(flowContentsBytes)) {
                return readVersionedFlowSnapshot(in);
            } catch (final Exception e) {
                throw new IOException("Configuration includes escaped JSON contents but failed to parse the dataflow", e);
            }
        }

        // Try downloading flow from registry
        final String registryUrl = properties.get(REGISTRY_URL_KEY);
        final String bucketId = properties.get(BUCKET_ID_KEY);
        final String flowId = properties.get(FLOW_ID_KEY);
        final String flowVersionValue = properties.get(FLOW_VERSION_KEY);
        final Integer flowVersion;
        try {
            flowVersion = flowVersionValue == null || flowVersionValue.trim().isEmpty() ? null : Integer.parseInt(flowVersionValue);
        } catch (final NumberFormatException nfe) {
            throw new StatelessConfigurationException("The " + FLOW_VERSION_KEY + " property was expected to contain a number but had a value of " + flowVersionValue);
        }

        if (registryUrl == null || bucketId == null || flowId == null) {
            throw new IllegalArgumentException("Configuration does not provide the filename of the flow to run, a URL to fetch it from, or the registryUrl, bucketId, and flowId.");
        }

        try {
            final SSLContext sslContext = SslConfigurationUtil.createSslContext(sslContextDefinition);
            return fetchFlowFromRegistry(registryUrl, bucketId, flowId, flowVersion, sslContext);
        } catch (final NiFiRegistryException e) {
            throw new StatelessConfigurationException("Could not fetch flow from Registry", e);
        }
    }

    private VersionedFlowSnapshot fetchFlowFromUrl(final String url, final SslContextDefinition sslContextDefinition) throws IOException {
        final OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
            .callTimeout(30, TimeUnit.SECONDS);

        if (sslContextDefinition != null) {
            final TlsConfiguration tlsConfiguration = SslConfigurationUtil.createTlsConfiguration(sslContextDefinition);
            OkHttpClientUtils.applyTlsToOkHttpClientBuilder(tlsConfiguration, clientBuilder);
        }

        final OkHttpClient client = clientBuilder.build();

        final Request getRequest = new Request.Builder()
            .url(url)
            .get()
            .build();

        final Call call = client.newCall(getRequest);

        try (final Response response = call.execute()) {
            final ResponseBody responseBody = response.body();

            if (!response.isSuccessful()) {
                final String responseText = responseBody == null ? "<No Message Received from Server>" : responseBody.string();
                throw new IOException("Failed to download flow from URL " + url + ": Response was " + response.code() + ": " + responseText);
            }

            if (responseBody == null) {
                throw new IOException("Failed to download flow from URL " + url + ": Received successful response code " + response.code() + " but no Response body");
            }

            try {
                final VersionedFlowSnapshot snapshot = OBJECT_MAPPER.readValue(responseBody.bytes(), VersionedFlowSnapshot.class);
                return snapshot;
            } catch (final Exception e) {
                throw new IOException("Downloaded flow from " + url + " but failed to parse the contents as a Versioned Flow. Please verify that the correct URL was provided.", e);
            }
        }
    }

    private VersionedFlowSnapshot fetchFlowFromRegistry(final String registryUrl, final String bucketId, final String flowId, final Integer flowVersion,
                                                        final SSLContext sslContext) throws IOException, NiFiRegistryException {

        logger.info("Fetching flow from NiFi Registry at {}", registryUrl);
        final long start = System.currentTimeMillis();

        final RegistryUtil registryUtil = new RegistryUtil(registryUrl, sslContext);
        final VersionedFlowSnapshot snapshot = registryUtil.getFlowByID(bucketId, flowId, flowVersion == null ? -1 : flowVersion);

        final long millis = System.currentTimeMillis() - start;
        logger.info("Successfully fetched flow from NiFi Registry in {} millis", millis);

        return snapshot;
    }

    private VersionedFlowSnapshot readVersionedFlowSnapshot(final File snapshotFile) throws IOException {
        try (final InputStream fis = new FileInputStream(snapshotFile)) {
            return readVersionedFlowSnapshot(fis);
        }
    }

    private VersionedFlowSnapshot readVersionedFlowSnapshot(final InputStream in) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();

        final VersionedFlowSnapshot snapshot = objectMapper.readValue(in, VersionedFlowSnapshot.class);

        final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
        metadata.setBucketIdentifier("external-bucket");
        metadata.setFlowIdentifier("external-flow");
        metadata.setTimestamp(System.currentTimeMillis());
        metadata.setVersion(1);
        snapshot.setSnapshotMetadata(metadata);

        return snapshot;
    }

}
