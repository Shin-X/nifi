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
package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@EventDriven
@SupportsBatching
@Tags({ "elasticsearch", "query", "scroll", "read", "get", "http" })
@CapabilityDescription("使用指定的连接属性滚动Elasticsearch查询。这个处理器打算在主节点上运行，并且设计用于滚动巨大的结果集，就像在重索引的情况下一样。在运行另一个查询之前，必须清除该状态。返回的每一页结果，都被包装在一个JSON对象中，如下所示: { \"hits\" : [ <doc1>, <doc2>, <docn> ] }。请注意，文档的每一页的正文在写入流文件进行传输之前都将被读入内存。")
@WritesAttributes({
        @WritesAttribute(attribute = "es.index", description = "The Elasticsearch index containing the document"),
        @WritesAttribute(attribute = "es.type", description = "The Elasticsearch document type") })
@DynamicProperty(
        name = "A URL query parameter",
        value = "The value to set it to",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing")
@Stateful(description = "After each successful scroll page, the latest scroll_id is persisted in scrollId as input for the next scroll call.  "
        + "Once the entire query is complete, finishedQuery state will be set to true, and the processor will not execute unless this is cleared.", scopes = { Scope.LOCAL })
public class ScrollElasticsearchHttp extends AbstractElasticsearchHttpProcessor {

    private static final String FINISHED_QUERY_STATE = "finishedQuery";
    private static final String SCROLL_ID_STATE = "scrollId";
    private static final String SCROLL_QUERY_PARAM = "scroll";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description(
                    "All FlowFiles that are read from Elasticsearch are routed to this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "All FlowFiles that cannot be read from Elasticsearch are routed to this relationship. Note that only incoming "
                            + "flow files will be routed to failure.").build();

    public static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("scroll-es-query")
            .displayName("Query")
            .description("The Lucene-style query to run against ElasticSearch (e.g., genre:blues AND -artist:muddy)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SCROLL_DURATION = new PropertyDescriptor.Builder()
            .name("scroll-es-scroll")
            .displayName("Scroll Duration")
            .description("The scroll duration is how long each search context is kept in memory.")
            .defaultValue("1m")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(
                    StandardValidators.createRegexMatchingValidator(Pattern.compile("[0-9]+(m|h)")))
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("scroll-es-index")
            .displayName("Index")
            .description("The name of the index to read from. If the property is set "
                            + "to _all, the query will match across all indexes.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("scroll-es-type")
            .displayName("Type")
            .description("The type of document (if unset, the query will be against all types in the _index). "
                    + "This should be unset or '_doc' for Elasticsearch 7.0+.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("scroll-es-fields")
            .displayName("Fields")
            .description(
                    "A comma-separated list of fields to retrieve from the document. If the Fields property is left blank, "
                            + "then the entire document's source will be retrieved.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SORT = new PropertyDescriptor.Builder()
            .name("scroll-es-sort")
            .displayName("Sort")
            .description(
                    "A sort parameter (e.g., timestamp:asc). If the Sort property is left blank, "
                            + "then the results will be retrieved in document order.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("scroll-es-size")
            .displayName("Page Size")
            .defaultValue("20")
            .description("Determines how many documents to return per page during scrolling.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>(COMMON_PROPERTY_DESCRIPTORS);
        descriptors.add(QUERY);
        descriptors.add(SCROLL_DURATION);
        descriptors.add(PAGE_SIZE);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(FIELDS);
        descriptors.add(SORT);

        propertyDescriptors = Collections.unmodifiableList(descriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        super.setup(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session)
            throws ProcessException {

        try {
            if (isQueryFinished(session)) {
                getLogger().trace(
                        "Query has been marked finished in the state manager.  "
                                + "To run another query, clear the state.");
                return;
            }
        } catch (IOException e) {
            throw new ProcessException("Could not retrieve state", e);
        }

        OkHttpClient okHttpClient = getClient();

        FlowFile flowFile = session.create();

        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile)
                .getValue();
        final String query = context.getProperty(QUERY).evaluateAttributeExpressions(flowFile)
                .getValue();
        final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile)
                .getValue();
        final int pageSize = context.getProperty(PAGE_SIZE).evaluateAttributeExpressions(flowFile)
                .asInteger();
        final String fields = context.getProperty(FIELDS).isSet() ? context.getProperty(FIELDS)
                .evaluateAttributeExpressions(flowFile).getValue() : null;
        final String sort = context.getProperty(SORT).isSet() ? context.getProperty(SORT)
                .evaluateAttributeExpressions(flowFile).getValue() : null;
        final String scroll = context.getProperty(SCROLL_DURATION).isSet() ? context
                .getProperty(SCROLL_DURATION).evaluateAttributeExpressions(flowFile).getValue() : null;

        // Authentication
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        final ComponentLog logger = getLogger();

        try {
            String scrollId = loadScrollId(session);

            // read the url property from the context
            final String urlstr = StringUtils.trimToEmpty(context.getProperty(ES_URL).evaluateAttributeExpressions()
                    .getValue());
            if (scrollId != null) {
                final URL scrollurl = buildRequestURL(urlstr, query, index, docType, fields, sort,
                        scrollId, pageSize, scroll, context);
                final long startNanos = System.nanoTime();

                final String scrollBody = String.format("{ \"scroll\": \"%s\", \"scroll_id\": \"%s\" }", scroll,
                        scrollId);

                final RequestBody body = RequestBody.create(MediaType.parse("application/json"), scrollBody);

                final Response getResponse = sendRequestToElasticsearch(okHttpClient, scrollurl,
                        username, password, "POST", body);
                this.getPage(getResponse, scrollurl, context, session, flowFile, logger, startNanos, charset);
                getResponse.close();
            } else {
                logger.debug("Querying {}/{} from Elasticsearch: {}", new Object[] { index,
                        docType, query });

                // read the url property from the context
                final URL queryUrl = buildRequestURL(urlstr, query, index, docType, fields, sort,
                        scrollId, pageSize, scroll, context);
                final long startNanos = System.nanoTime();

                final Response getResponse = sendRequestToElasticsearch(okHttpClient, queryUrl,
                        username, password, "GET", null);
                this.getPage(getResponse, queryUrl, context, session, flowFile, logger, startNanos, charset);
                getResponse.close();
            }

        } catch (IOException ioe) {
            logger.error(
                    "Failed to read from Elasticsearch due to {}, this may indicate an error in configuration "
                            + "(hosts, username/password, etc.).",
                    new Object[] { ioe.getLocalizedMessage() }, ioe);
            session.remove(flowFile);
            context.yield();

        } catch (Exception e) {
            logger.error("Failed to read {} from Elasticsearch due to {}", new Object[] { flowFile,
                    e.getLocalizedMessage() }, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    private void getPage(final Response getResponse, final URL url, final ProcessContext context,
            final ProcessSession session, FlowFile flowFile, final ComponentLog logger, final long startNanos, Charset charset)
            throws IOException {
        final int statusCode = getResponse.code();

        if (isSuccess(statusCode)) {
            ResponseBody body = getResponse.body();
            final byte[] bodyBytes = body.bytes();
            JsonNode responseJson = parseJsonResponse(new ByteArrayInputStream(bodyBytes));
            String scrollId = responseJson.get("_scroll_id").asText();

            StringBuilder builder = new StringBuilder();

            builder.append("{ \"hits\" : [");

            JsonNode hits = responseJson.get("hits").get("hits");
            if (hits.size() == 0) {
                finishQuery(context.getStateManager());
                session.remove(flowFile);
                return;
            }

            for(int i = 0; i < hits.size(); i++) {
                JsonNode hit = hits.get(i);
                String retrievedIndex = hit.get("_index").asText();
                String retrievedType = hit.get("_type").asText();

                JsonNode source = hit.get("_source");
                flowFile = session.putAttribute(flowFile, "es.index", retrievedIndex);
                flowFile = session.putAttribute(flowFile, "es.type", retrievedType);
                flowFile = session.putAttribute(flowFile, "mime.type", "application/json");

                builder.append(source.toString());
                if (i < hits.size() - 1) {
                    builder.append(", ");
                }
            }
            builder.append("] }");
            logger.debug("Elasticsearch retrieved " + responseJson.size() + " documents, routing to success");

            flowFile = session.write(flowFile, out -> {
                out.write(builder.toString().getBytes(charset));
            });
            session.transfer(flowFile, REL_SUCCESS);

            session.setState(Collections.singletonMap(SCROLL_ID_STATE, scrollId), Scope.LOCAL);

            // emit provenance event
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().receive(flowFile, url.toExternalForm(), millis);
        } else {
            // 5xx -> RETRY, but a server error might last a while, so yield
            if (statusCode / 100 == 5) {

                logger.warn("Elasticsearch returned code {} with message {}, removing the flow file. This is likely a server problem, yielding...",
                        new Object[]{statusCode, getResponse.message()});
                session.remove(flowFile);
                context.yield();
            }  else {
                logger.warn("Elasticsearch returned code {} with message {}", new Object[]{statusCode, getResponse.message()});
                session.remove(flowFile);
            }
        }
    }

    private boolean isQueryFinished(final ProcessSession session) throws IOException {
        final StateMap stateMap = session.getState(Scope.LOCAL);

        if (stateMap.getVersion() < 0) {
            getLogger().debug("No previous state found");
            return false;
        }

        final String isQueryFinished = stateMap.get(FINISHED_QUERY_STATE);
        getLogger().debug("Loaded state with finishedQuery = {}", new Object[] { isQueryFinished });

        return "true".equals(isQueryFinished);
    }

    private String loadScrollId(final ProcessSession session) throws IOException {
        final StateMap stateMap = session.getState(Scope.LOCAL);

        if (stateMap.getVersion() < 0) {
            getLogger().debug("No previous state found");
            return null;
        }

        final String scrollId = stateMap.get(SCROLL_ID_STATE);
        getLogger().debug("Loaded state with scrollId {}", new Object[] { scrollId });

        return scrollId;
    }

    private void finishQuery(StateManager stateManager) throws IOException {

        Map<String, String> state = new HashMap<>(2);
        state.put(FINISHED_QUERY_STATE, "true");

        getLogger().debug("Saving state with finishedQuery = true");
        stateManager.setState(state, Scope.LOCAL);
    }

    private URL buildRequestURL(String baseUrl, String query, String index, String type, String fields,
            String sort, String scrollId, int pageSize, String scroll, ProcessContext context) throws MalformedURLException {
        if (StringUtils.isEmpty(baseUrl)) {
            throw new MalformedURLException("Base URL cannot be null");
        }
        HttpUrl.Builder builder = HttpUrl.parse(baseUrl).newBuilder();
        if (!StringUtils.isEmpty(scrollId)) {
            builder.addPathSegment("_search");
            builder.addPathSegment("scroll");
        } else {
            builder.addPathSegment((StringUtils.isEmpty(index)) ? "_all" : index);
            if (StringUtils.isNotBlank(type)) {
                builder.addPathSegment(type);
            }
            builder.addPathSegment("_search");
            builder.addQueryParameter(QUERY_QUERY_PARAM, query);
            builder.addQueryParameter(SIZE_QUERY_PARAM, String.valueOf(pageSize));
            if (!StringUtils.isEmpty(fields)) {
                String trimmedFields = Stream.of(fields.split(",")).map(String::trim).collect(Collectors.joining(","));
                builder.addQueryParameter(SOURCE_QUERY_PARAM, trimmedFields);
            }
            if (!StringUtils.isEmpty(sort)) {
                String trimmedFields = Stream.of(sort.split(",")).map(String::trim).collect(Collectors.joining(","));
                builder.addQueryParameter(SORT_QUERY_PARAM, trimmedFields);
            }
            builder.addQueryParameter(SCROLL_QUERY_PARAM, scroll);
        }

        // Find the user-added properties and set them as query parameters on the URL
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            PropertyDescriptor pd = property.getKey();
            if (pd.isDynamic()) {
                if (property.getValue() != null) {
                    builder.addQueryParameter(pd.getName(), context.getProperty(pd).evaluateAttributeExpressions().getValue());
                }
            }
        }


        return builder.build().url();
    }
}
