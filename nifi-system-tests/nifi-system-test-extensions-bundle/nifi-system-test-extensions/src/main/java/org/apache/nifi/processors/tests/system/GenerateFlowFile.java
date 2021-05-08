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
package org.apache.nifi.processors.tests.system;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.nifi.expression.ExpressionLanguageScope.VARIABLE_REGISTRY;

@DefaultSchedule(period = "10 mins")
public class GenerateFlowFile extends AbstractProcessor {
    public static final PropertyDescriptor FILE_SIZE = new Builder()
        .name("File Size")
        .description("The size of the file that will be used")
        .required(true)
        .defaultValue("0 B")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();
    public static final PropertyDescriptor BATCH_SIZE = new Builder()
        .name("Batch Size")
        .description("The number of FlowFiles to be transferred in each invocation")
        .required(true)
        .defaultValue("1")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();
    public static final PropertyDescriptor CUSTOM_TEXT = new Builder()
        .name("Text")
        .description("If Data Format is text and if Unique FlowFiles is false, then this custom text will be used as content of the generated "
            + "FlowFiles and the File Size will be ignored. Finally, if Expression Language is used, evaluation will be performed only once "
            + "per batch of generated FlowFiles")
        .required(false)
        .expressionLanguageSupported(VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor STATE_SCOPE = new Builder()
        .name("State Scope")
        .displayName("State Scope")
        .description("Whether to store state locally or in cluster")
        .required(false)
        .allowableValues("LOCAL", "CLUSTER")
        .defaultValue("LOCAL")
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Arrays.asList(FILE_SIZE, BATCH_SIZE, CUSTOM_TEXT, STATE_SCOPE);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(VARIABLE_REGISTRY)
            .dynamic(true)
            .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int numFlowFiles = context.getProperty(BATCH_SIZE).asInteger();

        for (int i=0; i < numFlowFiles; i++) {
            final FlowFile flowFile = createFlowFile(context, session);
            session.transfer(flowFile, REL_SUCCESS);
        }

        getLogger().info("Generated {} FlowFiles", new Object[] {numFlowFiles});
    }

    private FlowFile createFlowFile(final ProcessContext context, final ProcessSession session) {
        final Scope scope = Scope.valueOf(context.getProperty(STATE_SCOPE).getValue().toUpperCase());
        final StateMap stateMap;
        try {
            stateMap = session.getState(scope);
        } catch (final IOException e) {
            throw new ProcessException(e);
        }

        FlowFile flowFile = session.create();

        final Map<String, String> attributes = new HashMap<>();
        context.getProperties().keySet().forEach(descriptor -> {
            if (descriptor.isDynamic()) {
                final String value = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
                attributes.put(descriptor.getName(), value);
            }
        });

        if (!attributes.isEmpty()) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }

        final String customText = context.getProperty(CUSTOM_TEXT).evaluateAttributeExpressions().getValue();
        if (customText == null) {
            final int dataSize = context.getProperty(FILE_SIZE).asDataSize(DataUnit.B).intValue();
            if (dataSize > 0L) {
                final byte[] data = new byte[dataSize];
                final Random random = new Random();
                random.nextBytes(data);

                flowFile = session.write(flowFile, out -> out.write(data));
            }
        } else {
            flowFile = session.write(flowFile, out -> out.write(customText.getBytes(StandardCharsets.UTF_8)));
        }

        final String countValue = stateMap.toMap().get("count");
        final int count = countValue == null ? 0 : Integer.parseInt(countValue);
        try {
            session.setState(Collections.singletonMap("count", String.valueOf(count + 1)), scope);
        } catch (final IOException e) {
            throw new ProcessException(e);
        }

        return flowFile;
    }
}
