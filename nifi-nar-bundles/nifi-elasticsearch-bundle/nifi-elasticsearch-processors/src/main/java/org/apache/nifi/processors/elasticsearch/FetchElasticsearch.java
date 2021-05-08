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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@EventDriven
@SupportsBatching
@Tags({"elasticsearch", "fetch", "read", "get"})
@CapabilityDescription("使用指定的连接属性和要检索的文档的标识符从Elasticsearch检索文档。如果集群已配置为授权和/或安全传输(SSL/TLS)，并且屏蔽插件可用，则可以建立安全连接。这个处理器支持Elasticsearch 2。x集群。")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The filename attributes is set to the document identifier"),
        @WritesAttribute(attribute = "es.index", description = "The Elasticsearch index containing the document"),
        @WritesAttribute(attribute = "es.type", description = "The Elasticsearch document type")
})
public class FetchElasticsearch extends AbstractElasticsearchTransportClientProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are read from Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be read from Elasticsearch are routed to this relationship").build();

    public static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
            .description("A FlowFile is routed to this relationship if the document cannot be fetched but attempting the operation again may succeed")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder().name("not found")
            .description("A FlowFile is routed to this relationship if the specified document does not exist in the Elasticsearch cluster")
            .build();

    public static final PropertyDescriptor DOC_ID = new PropertyDescriptor.Builder()
            .name("Document Identifier")
            .description("The identifier for the document to be fetched")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
            .name("Index")
            .description("The name of the index to read from")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("Type")
            .description("The type of this document (used by Elasticsearch for indexing and searching)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        final Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_RETRY);
        _rels.add(REL_NOT_FOUND);
        relationships = Collections.unmodifiableSet(_rels);

        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CLUSTER_NAME);
        descriptors.add(HOSTS);
        descriptors.add(PROP_SSL_CONTEXT_SERVICE);
        descriptors.add(PROP_SHIELD_LOCATION);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(PING_TIMEOUT);
        descriptors.add(SAMPLER_INTERVAL);
        descriptors.add(DOC_ID);
        descriptors.add(INDEX);
        descriptors.add(TYPE);
        descriptors.add(CHARSET);

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

    @Override
    @OnScheduled
    public void setup(ProcessContext context) {
        super.setup(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile).getValue();
        final String docId = context.getProperty(DOC_ID).evaluateAttributeExpressions(flowFile).getValue();
        final String docType = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile).getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue());

        final ComponentLog logger = getLogger();
        try {

            logger.debug("Fetching {}/{}/{} from Elasticsearch", new Object[]{index, docType, docId});
            final long startNanos = System.nanoTime();

            GetRequestBuilder getRequestBuilder = esClient.get().prepareGet(index, docType, docId);
            if (authToken != null) {
                getRequestBuilder.putHeader("Authorization", authToken);
            }
            final GetResponse getResponse = getRequestBuilder.execute().actionGet();

            if (getResponse == null || !getResponse.isExists()) {
                logger.debug("Failed to read {}/{}/{} from Elasticsearch: Document not found",
                        new Object[]{index, docType, docId});

                // We couldn't find the document, so penalize it and send it to "not found"
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                flowFile = session.putAttribute(flowFile, "filename", docId);
                flowFile = session.putAttribute(flowFile, "es.index", index);
                flowFile = session.putAttribute(flowFile, "es.type", docType);
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(getResponse.getSourceAsString().getBytes(charset));
                    }
                });
                logger.debug("Elasticsearch document " + docId + " fetched, routing to success");

                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                final String uri = context.getProperty(HOSTS).evaluateAttributeExpressions().getValue() + "/" + index + "/" + docType + "/" + docId;
                session.getProvenanceReporter().fetch(flowFile, uri, millis);
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (NoNodeAvailableException
                | ElasticsearchTimeoutException
                | ReceiveTimeoutTransportException
                | NodeClosedException exceptionToRetry) {
            logger.error("Failed to read into Elasticsearch due to {}, this may indicate an error in configuration "
                            + "(hosts, username/password, etc.). Routing to retry",
                    new Object[]{exceptionToRetry.getLocalizedMessage()}, exceptionToRetry);
            session.transfer(flowFile, REL_RETRY);
            context.yield();

        } catch (Exception e) {
            logger.error("Failed to read {} from Elasticsearch due to {}", new Object[]{flowFile, e.getLocalizedMessage()}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    /**
     * Dispose of ElasticSearch client
     */
    @Override
    @OnStopped
    public void closeClient() {
        super.closeClient();
    }
}
