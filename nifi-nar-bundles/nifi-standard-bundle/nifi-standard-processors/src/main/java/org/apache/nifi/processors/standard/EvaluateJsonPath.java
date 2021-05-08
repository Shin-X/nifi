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
package org.apache.nifi.processors.standard;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import java.util.stream.Collectors;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"JSON", "evaluate", "JsonPath"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("根据流文件的内容计算一个或多个JsonPath表达式。根据处理器的配置，这些表达式的结果被赋值给流文件属性，或者被写入流文件本身的内容。" +
        "通过添加用户定义的属性来输入jsonpath;属性的名称映射到将放置结果的属性名(如果目标是flowfile-attribute;否则，将忽略属性名)。属性的值必须是有效的JsonPath表达式。" +
        "返回类型“auto-detect”将基于配置的目的地做出判断。当'Destination'设置为'flowfile-attribute '时，将使用'scalar'的返回类型。" +
        "当'Destination'设置为'flowfile-content '时，将使用'JSON'的返回类型。如果JsonPath计算为一个JSON数组或JSON对象，并且返回类型设置为'scalar'，" +
        "则流文件将不被修改，并将被路由到失败。如果提供的JsonPath计算为指定的值并将作为匹配进行路由，则JSON返回类型可以返回标量值。如果目的地是' FlowFile -content'，" +
        "并且JsonPath没有计算到定义的路径，那么FlowFile将被路由到'unmatched'，而不需要修改其内容。如果Destination是FlowFile -attribute且表达式不匹配任何内容，" +
        "则属性将以空字符串作为值创建，并且FlowFile将始终被路由到'matched '。")
//@CapabilityDescription("Evaluates one or more JsonPath expressions against the content of a FlowFile. "
//        + "The results of those expressions are assigned to FlowFile Attributes or are written to the content of the FlowFile itself, "
//        + "depending on configuration of the Processor. "
//        + "JsonPaths are entered by adding user-defined properties; the name of the property maps to the Attribute Name "
//        + "into which the result will be placed (if the Destination is flowfile-attribute; otherwise, the property name is ignored). "
//        + "The value of the property must be a valid JsonPath expression. "
//        + "A Return Type of 'auto-detect' will make a determination based off the configured destination. "
//        + "When 'Destination' is set to 'flowfile-attribute,' a return type of 'scalar' will be used. "
//        + "When 'Destination' is set to 'flowfile-content,' a return type of 'JSON' will be used."
//        + "If the JsonPath evaluates to a JSON array or JSON object and the Return Type is set to 'scalar' the FlowFile will be unmodified and will be routed to failure. "
//        + "A Return Type of JSON can return scalar values if the provided JsonPath evaluates to the specified value and will be routed as a match."
//        + "If Destination is 'flowfile-content' and the JsonPath does not evaluate to a defined path, the FlowFile will be routed to 'unmatched' without having its contents modified. "
//        + "If Destination is flowfile-attribute and the expression matches nothing, attributes will be created with "
//        + "empty strings as the value, and the FlowFile will always be routed to 'matched.'")
@DynamicProperty(name = "A FlowFile attribute(if <Destination> is set to 'flowfile-attribute')",
        value = "A JsonPath expression", description = "If <Destination>='flowfile-attribute' then that FlowFile attribute "
        + "will be set to any JSON objects that match the JsonPath.  If <Destination>='flowfile-content' then the FlowFile "
        + "content will be updated to any JSON objects that match the JsonPath.")
public class EvaluateJsonPath extends AbstractJsonPathProcessor {

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";

    public static final String RETURN_TYPE_AUTO = "auto-detect";
    public static final String RETURN_TYPE_JSON = "json";
    public static final String RETURN_TYPE_SCALAR = "scalar";

    public static final String PATH_NOT_FOUND_IGNORE = "ignore";
    public static final String PATH_NOT_FOUND_WARN = "warn";

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Indicates whether the results of the JsonPath evaluation are written to the FlowFile content or a FlowFile attribute; "
                    + "if using attribute, must specify the Attribute Name property. If set to flowfile-content, only one JsonPath may be specified, "
                    + "and the property name is ignored.")
            .required(true)
            .allowableValues(DESTINATION_CONTENT, DESTINATION_ATTRIBUTE)
            .defaultValue(DESTINATION_CONTENT)
            .build();

    public static final PropertyDescriptor RETURN_TYPE = new PropertyDescriptor.Builder()
            .name("Return Type").description("Indicates the desired return type of the JSON Path expressions.  Selecting 'auto-detect' will set the return type to 'json' "
                    + "for a Destination of 'flowfile-content', and 'scalar' for a Destination of 'flowfile-attribute'.")
            .required(true)
            .allowableValues(RETURN_TYPE_AUTO, RETURN_TYPE_JSON, RETURN_TYPE_SCALAR)
            .defaultValue(RETURN_TYPE_AUTO)
            .build();

    public static final PropertyDescriptor PATH_NOT_FOUND = new PropertyDescriptor.Builder()
            .name("Path Not Found Behavior")
            .description("Indicates how to handle missing JSON path expressions when destination is set to 'flowfile-attribute'. Selecting 'warn' will "
                    + "generate a warning when a JSON path expression is not found.")
            .required(true)
            .allowableValues(PATH_NOT_FOUND_WARN, PATH_NOT_FOUND_IGNORE)
            .defaultValue(PATH_NOT_FOUND_IGNORE)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles are routed to this relationship when the JsonPath is successfully evaluated and the FlowFile is modified as a result")
            .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles are routed to this relationship when the JsonPath does not match the content of the FlowFile and the Destination is set to flowfile-content")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship when the JsonPath cannot be evaluated against the content of the "
                    + "FlowFile; for instance, if the FlowFile is not valid JSON")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final ConcurrentMap<String, JsonPath> cachedJsonPathMap = new ConcurrentHashMap<>();

    private final Queue<Set<Map.Entry<String, JsonPath>>> attributeToJsonPathEntrySetQueue = new ConcurrentLinkedQueue<>();
    private volatile String representationOption;
    private volatile boolean destinationIsAttribute;
    private volatile String returnType;
    private volatile String pathNotFound;
    private volatile String nullDefaultValue;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
        properties.add(RETURN_TYPE);
        properties.add(PATH_NOT_FOUND);
        properties.add(NULL_VALUE_DEFAULT_REPRESENTATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(context));

        final String destination = context.getProperty(DESTINATION).getValue();
        if (DESTINATION_CONTENT.equals(destination)) {
            int jsonPathCount = 0;

            for (final PropertyDescriptor desc : context.getProperties().keySet()) {
                if (desc.isDynamic()) {
                    jsonPathCount++;
                }
            }

            if (jsonPathCount != 1) {
                results.add(new ValidationResult.Builder().subject("JsonPaths").valid(false)
                        .explanation("Exactly one JsonPath must be set if using destination of " + DESTINATION_CONTENT).build());
            }
        }

        return results;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .addValidator(
                        new JsonPathValidator() {
                            @Override
                            public void cacheComputedValue(String subject, String input, JsonPath computedJsonPath) {
                                cachedJsonPathMap.put(input, computedJsonPath);
                            }

                            @Override
                            public boolean isStale(String subject, String input) {
                                return cachedJsonPathMap.get(input) == null;
                            }
                })
                .required(false).dynamic(true).build();
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (descriptor.isDynamic()) {
            if (!StringUtils.equals(oldValue, newValue)) {
                if (oldValue != null) {
                    cachedJsonPathMap.remove(oldValue);
                }
            }
        }
    }

    /**
     * Provides cleanup of the map for any JsonPath values that may have been created. This will remove common values shared between multiple instances, but will be regenerated when the next
     * validation cycle occurs as a result of isStale()
     *
     * @param processContext context
     */
    @OnRemoved
    public void onRemoved(ProcessContext processContext) {
        for (PropertyDescriptor propertyDescriptor : getPropertyDescriptors()) {
            if (propertyDescriptor.isDynamic()) {
                cachedJsonPathMap.remove(processContext.getProperty(propertyDescriptor).getValue());
            }
        }
    }

    @OnScheduled
    public void onScheduled(ProcessContext processContext) {
        representationOption = processContext.getProperty(NULL_VALUE_DEFAULT_REPRESENTATION).getValue();
        destinationIsAttribute = DESTINATION_ATTRIBUTE.equals(processContext.getProperty(DESTINATION).getValue());
        returnType = processContext.getProperty(RETURN_TYPE).getValue();
        if (returnType.equals(RETURN_TYPE_AUTO)) {
            returnType = destinationIsAttribute ? RETURN_TYPE_SCALAR : RETURN_TYPE_JSON;
        }
        pathNotFound = processContext.getProperty(PATH_NOT_FOUND).getValue();
        nullDefaultValue = NULL_REPRESENTATION_MAP.get(representationOption);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        attributeToJsonPathEntrySetQueue.clear();
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        DocumentContext documentContext;
        try {
            documentContext = validateAndEstablishJsonContext(processSession, flowFile);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{flowFile});
            processSession.transfer(flowFile, REL_FAILURE);
            return;
        }

        Set<Map.Entry<String, JsonPath>> attributeJsonPathEntries = attributeToJsonPathEntrySetQueue.poll();
        if (attributeJsonPathEntries == null) {
            attributeJsonPathEntries = processContext.getProperties().entrySet().stream()
                    .filter(e -> e.getKey().isDynamic())
                    .collect(Collectors.toMap(e -> e.getKey().getName(), e -> JsonPath.compile(e.getValue())))
                    .entrySet();
        }

        try {
            // We'll only be using this map if destinationIsAttribute == true
            final Map<String, String> jsonPathResults = destinationIsAttribute ? new HashMap<>(attributeJsonPathEntries.size()) : Collections.EMPTY_MAP;

            for (final Map.Entry<String, JsonPath> attributeJsonPathEntry : attributeJsonPathEntries) {
                final String jsonPathAttrKey = attributeJsonPathEntry.getKey();
                final JsonPath jsonPathExp = attributeJsonPathEntry.getValue();

                Object result;
                try {
                    Object potentialResult = documentContext.read(jsonPathExp);
                    if (returnType.equals(RETURN_TYPE_SCALAR) && !isJsonScalar(potentialResult)) {
                        logger.error("Unable to return a scalar value for the expression {} for FlowFile {}. Evaluated value was {}. Transferring to {}.",
                                new Object[]{jsonPathExp.getPath(), flowFile.getId(), potentialResult.toString(), REL_FAILURE.getName()});
                        processSession.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                    result = potentialResult;
                } catch (PathNotFoundException e) {
                    if (pathNotFound.equals(PATH_NOT_FOUND_WARN)) {
                        logger.warn("FlowFile {} could not find path {} for attribute key {}.",
                                new Object[]{flowFile.getId(), jsonPathExp.getPath(), jsonPathAttrKey}, e);
                    }

                    if (destinationIsAttribute) {
                        jsonPathResults.put(jsonPathAttrKey, StringUtils.EMPTY);
                        continue;
                    } else {
                        processSession.transfer(flowFile, REL_NO_MATCH);
                        return;
                    }
                }

                final String resultRepresentation = getResultRepresentation(result, nullDefaultValue);
                if (destinationIsAttribute) {
                    jsonPathResults.put(jsonPathAttrKey, resultRepresentation);
                } else {
                    flowFile = processSession.write(flowFile, out -> {
                        try (OutputStream outputStream = new BufferedOutputStream(out)) {
                            outputStream.write(resultRepresentation.getBytes(StandardCharsets.UTF_8));
                        }
                    });
                    processSession.getProvenanceReporter().modifyContent(flowFile, "Replaced content with result of expression " + jsonPathExp.getPath());
                }
            }

            // jsonPathResults map will be empty if this is false
            if (destinationIsAttribute) {
                flowFile = processSession.putAllAttributes(flowFile, jsonPathResults);
            }
            processSession.transfer(flowFile, REL_MATCH);
        } finally {
            attributeToJsonPathEntrySetQueue.offer(attributeJsonPathEntries);
        }
    }
}
