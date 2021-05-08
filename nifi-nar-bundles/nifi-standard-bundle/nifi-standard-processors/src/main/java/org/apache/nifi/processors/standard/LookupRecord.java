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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile")
})
@Tags({"lookup", "enrichment", "route", "record", "csv", "json", "avro", "database", "db", "logs", "convert", "filter"})
@CapabilityDescription("从记录中提取一个或多个字段，并在LookupService中查找这些字段的值。如果LookupService返回一个结果，则该结果可选地添加到记录中。在这种情况下，处理器的功能相当于一个浓缩处理器。无论如何，记录都会被路由到“匹配的”关系或“不匹配的”关系(如果“路由策略”属性被配置为这样做的话)，指示结果是否由LookupService返回，从而允许处理器也作为路由处理器。用于在查找服务中查找值的“坐标”是通过添加用户定义的属性定义的。添加的每个属性都将有一个条目添加到映射中，其中属性的名称成为映射键，由RecordPath返回的值成为该键的值。如果RecordPath返回多个值，则该记录将被路由到“不匹配”关系(或“成功”，这取决于“路由策略”属性的配置)。如果一个或多个字段与结果RecordPath匹配，那么所有匹配的字段都将被更新。如果配置的LookupService中没有匹配项，则不会更新任何字段。也就是说，它不会用空值覆盖记录中现有的值。然而，请注意，如果LookupService返回的结果没有在您的模式中(特别是为记录写入器配置的模式)，那么字段将不会被写入流文件。")
@DynamicProperty(name = "Value To Lookup", value = "Valid Record Path", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                    description = "A RecordPath that points to the field whose value will be looked up in the configured Lookup Service")
@SeeAlso(value = {ConvertRecord.class, SplitRecord.class},
        classNames = {"org.apache.nifi.lookup.SimpleKeyValueLookupService", "org.apache.nifi.lookup.maxmind.IPLookupService", "org.apache.nifi.lookup.db.DatabaseRecordLookupService"})
public class LookupRecord extends AbstractRouteRecord<Tuple<Map<String, RecordPath>, RecordPath>> {

    private volatile RecordPathCache recordPathCache = new RecordPathCache(25);
    private volatile LookupService<?> lookupService;

    static final AllowableValue ROUTE_TO_SUCCESS = new AllowableValue("route-to-success", "Route to 'success'",
        "Records will be routed to a 'success' Relationship regardless of whether or not there is a match in the configured Lookup Service");
    static final AllowableValue ROUTE_TO_MATCHED_UNMATCHED = new AllowableValue("route-to-matched-unmatched", "Route to 'matched' or 'unmatched'",
        "Records will be routed to either a 'matched' or an 'unmatched' Relationship depending on whether or not there was a match in the configured Lookup Service. "
            + "A single input FlowFile may result in two different output FlowFiles.");

    static final AllowableValue RESULT_ENTIRE_RECORD = new AllowableValue("insert-entire-record", "Insert Entire Record",
        "The entire Record that is retrieved from the Lookup Service will be inserted into the destination path.");
    static final AllowableValue RESULT_RECORD_FIELDS = new AllowableValue("record-fields", "Insert Record Fields",
        "All of the fields in the Record that is retrieved from the Lookup Service will be inserted into the destination path.");

    static final AllowableValue USE_PROPERTY = new AllowableValue("use-property", "Use Property",
            "The \"Result RecordPath\" property will be used to determine which part of the record should be updated with the value returned by the Lookup Service");
    static final AllowableValue REPLACE_EXISTING_VALUES = new AllowableValue("replace-existing-values", "Replace Existing Values",
            "The \"Result RecordPath\" property will be ignored and the lookup service must be a single simple key lookup service. Every dynamic property value should "
            + "be a record path. For each dynamic property, the value contained in the field corresponding to the record path will be used as the key in the Lookup "
            + "Service and the value returned by the Lookup Service will be used to replace the existing value. It is possible to configure multiple dynamic properties "
            + "to replace multiple values in one execution. This strategy only supports simple types replacements (strings, integers, etc).");

    static final PropertyDescriptor LOOKUP_SERVICE = new PropertyDescriptor.Builder()
        .name("lookup-service")
        .displayName("Lookup Service")
        .description("The Lookup Service to use in order to lookup a value in each Record")
        .identifiesControllerService(LookupService.class)
        .required(true)
        .build();

    static final PropertyDescriptor RESULT_RECORD_PATH = new PropertyDescriptor.Builder()
        .name("result-record-path")
        .displayName("Result RecordPath")
        .description("A RecordPath that points to the field whose value should be updated with whatever value is returned from the Lookup Service. "
            + "If not specified, the value that is returned from the Lookup Service will be ignored, except for determining whether the FlowFile should "
            + "be routed to the 'matched' or 'unmatched' Relationship.")
        .addValidator(new RecordPathValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .build();

    static final PropertyDescriptor RESULT_CONTENTS = new PropertyDescriptor.Builder()
        .name("result-contents")
        .displayName("Record Result Contents")
        .description("When a result is obtained that contains a Record, this property determines whether the Record itself is inserted at the configured "
            + "path or if the contents of the Record (i.e., the sub-fields) will be inserted at the configured path.")
        .allowableValues(RESULT_ENTIRE_RECORD, RESULT_RECORD_FIELDS)
        .defaultValue(RESULT_ENTIRE_RECORD.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor ROUTING_STRATEGY = new PropertyDescriptor.Builder()
        .name("routing-strategy")
        .displayName("Routing Strategy")
        .description("Specifies how to route records after a Lookup has completed")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(ROUTE_TO_SUCCESS, ROUTE_TO_MATCHED_UNMATCHED)
        .defaultValue(ROUTE_TO_SUCCESS.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor REPLACEMENT_STRATEGY = new PropertyDescriptor.Builder()
        .name("record-update-strategy")
        .displayName("Record Update Strategy")
        .description("This property defines the strategy to use when updating the record with the value returned by the Lookup Service.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(REPLACE_EXISTING_VALUES, USE_PROPERTY)
        .defaultValue(USE_PROPERTY.getValue())
        .required(true)
        .build();

    static final Relationship REL_MATCHED = new Relationship.Builder()
        .name("matched")
        .description("All records for which the lookup returns a value will be routed to this relationship")
        .build();
    static final Relationship REL_UNMATCHED = new Relationship.Builder()
        .name("unmatched")
        .description("All records for which the lookup does not have a matching value will be routed to this relationship")
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All records will be sent to this Relationship if configured to do so, unless a failure occurs")
        .build();

    private static final Set<Relationship> MATCHED_COLLECTION = Collections.singleton(REL_MATCHED);
    private static final Set<Relationship> UNMATCHED_COLLECTION = Collections.singleton(REL_UNMATCHED);
    private static final Set<Relationship> SUCCESS_COLLECTION = Collections.singleton(REL_SUCCESS);

    private volatile Set<Relationship> relationships = new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE));
    private volatile boolean routeToMatchedUnmatched = false;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.lookupService = context.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.addAll(super.getSupportedPropertyDescriptors());
        properties.add(LOOKUP_SERVICE);
        properties.add(RESULT_RECORD_PATH);
        properties.add(ROUTING_STRATEGY);
        properties.add(RESULT_CONTENTS);
        properties.add(REPLACEMENT_STRATEGY);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("A RecordPath that points to the field whose value will be looked up in the configured Lookup Service")
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .dynamic(true)
            .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final Set<String> dynamicPropNames = validationContext.getProperties().keySet().stream()
            .filter(PropertyDescriptor::isDynamic)
            .map(PropertyDescriptor::getName)
            .collect(Collectors.toSet());

        if (dynamicPropNames.isEmpty()) {
            return Collections.singleton(new ValidationResult.Builder()
                .subject("User-Defined Properties")
                .valid(false)
                .explanation("At least one user-defined property must be specified.")
                .build());
        }

        final Set<String> requiredKeys = validationContext.getProperty(LOOKUP_SERVICE).asControllerService(LookupService.class).getRequiredKeys();

        if(validationContext.getProperty(REPLACEMENT_STRATEGY).getValue().equals(REPLACE_EXISTING_VALUES.getValue())) {
            // it must be a single key lookup service
            if(requiredKeys.size() != 1) {
                return Collections.singleton(new ValidationResult.Builder()
                        .subject(LOOKUP_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("When using \"" + REPLACE_EXISTING_VALUES.getDisplayName() + "\" as Record Update Strategy, "
                                + "only a Lookup Service requiring a single key can be used.")
                        .build());
            }
        } else {
            final Set<String> missingKeys = requiredKeys.stream()
                .filter(key -> !dynamicPropNames.contains(key))
                .collect(Collectors.toSet());

            if (!missingKeys.isEmpty()) {
                final List<ValidationResult> validationResults = new ArrayList<>();
                for (final String missingKey : missingKeys) {
                    final ValidationResult result = new ValidationResult.Builder()
                        .subject(missingKey)
                        .valid(false)
                        .explanation("The configured Lookup Services requires that a key be provided with the name '" + missingKey
                            + "'. Please add a new property to this Processor with a name '" + missingKey
                            + "' and provide a RecordPath that can be used to retrieve the appropriate value.")
                        .build();
                    validationResults.add(result);
                }

                return validationResults;
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (ROUTING_STRATEGY.equals(descriptor)) {
            if (ROUTE_TO_MATCHED_UNMATCHED.getValue().equalsIgnoreCase(newValue)) {
                final Set<Relationship> matchedUnmatchedRels = new HashSet<>();
                matchedUnmatchedRels.add(REL_MATCHED);
                matchedUnmatchedRels.add(REL_UNMATCHED);
                matchedUnmatchedRels.add(REL_FAILURE);
                this.relationships = matchedUnmatchedRels;

                this.routeToMatchedUnmatched = true;
            } else {
                final Set<Relationship> successRels = new HashSet<>();
                successRels.add(REL_SUCCESS);
                successRels.add(REL_FAILURE);
                this.relationships = successRels;

                this.routeToMatchedUnmatched = false;
            }
        }
    }

    @Override
    protected Set<Relationship> route(final Record record, final RecordSchema writeSchema, final FlowFile flowFile, final ProcessContext context,
        final Tuple<Map<String, RecordPath>, RecordPath> flowFileContext) {

        final boolean isInPlaceReplacement = context.getProperty(REPLACEMENT_STRATEGY).getValue().equals(REPLACE_EXISTING_VALUES.getValue());

        if(isInPlaceReplacement) {
            return doInPlaceReplacement(record, flowFile, context, flowFileContext);
        } else {
            return doResultPathReplacement(record, flowFile, context, flowFileContext);
        }

    }

    private Set<Relationship> doInPlaceReplacement(Record record, FlowFile flowFile, ProcessContext context, Tuple<Map<String, RecordPath>, RecordPath> flowFileContext) {
        final Map<String, RecordPath> recordPaths = flowFileContext.getKey();
        final Map<String, Object> lookupCoordinates = new HashMap<>(recordPaths.size());

        for (final Map.Entry<String, RecordPath> entry : recordPaths.entrySet()) {
            final String coordinateKey = entry.getKey();
            final RecordPath recordPath = entry.getValue();

            final RecordPathResult pathResult = recordPath.evaluate(record);
            final List<FieldValue> lookupFieldValues = pathResult.getSelectedFields()
                .filter(fieldVal -> fieldVal.getValue() != null)
                .collect(Collectors.toList());

            if (lookupFieldValues.isEmpty()) {
                final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                getLogger().debug("RecordPath for property '{}' did not match any fields in a record for {}; routing record to {}", new Object[] {coordinateKey, flowFile, rels});
                return rels;
            }

            for (FieldValue fieldValue : lookupFieldValues) {
                final Object coordinateValue = (fieldValue.getValue() instanceof Number || fieldValue.getValue() instanceof Boolean)
                        ? fieldValue.getValue() : DataTypeUtils.toString(fieldValue.getValue(), (String) null);

                lookupCoordinates.clear();
                lookupCoordinates.put(coordinateKey, coordinateValue);

                final Optional<?> lookupValueOption;
                try {
                    lookupValueOption = lookupService.lookup(lookupCoordinates, flowFile.getAttributes());
                } catch (final Exception e) {
                    throw new ProcessException("Failed to lookup coordinates " + lookupCoordinates + " in Lookup Service", e);
                }

                if (!lookupValueOption.isPresent()) {
                    final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                    return rels;
                }

                final Object lookupValue = lookupValueOption.get();

                final DataType inferredDataType = DataTypeUtils.inferDataType(lookupValue, RecordFieldType.STRING.getDataType());
                fieldValue.updateValue(lookupValue, inferredDataType);

            }
        }

        final Set<Relationship> rels = routeToMatchedUnmatched ? MATCHED_COLLECTION : SUCCESS_COLLECTION;
        return rels;
    }

    private Set<Relationship> doResultPathReplacement(Record record, FlowFile flowFile, ProcessContext context, Tuple<Map<String, RecordPath>, RecordPath> flowFileContext) {
        final Map<String, RecordPath> recordPaths = flowFileContext.getKey();
        final Map<String, Object> lookupCoordinates = new HashMap<>(recordPaths.size());

        for (final Map.Entry<String, RecordPath> entry : recordPaths.entrySet()) {
            final String coordinateKey = entry.getKey();
            final RecordPath recordPath = entry.getValue();

            final RecordPathResult pathResult = recordPath.evaluate(record);
            final List<FieldValue> lookupFieldValues = pathResult.getSelectedFields()
                .filter(fieldVal -> fieldVal.getValue() != null)
                .collect(Collectors.toList());

            if (lookupFieldValues.isEmpty()) {
                final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                getLogger().debug("RecordPath for property '{}' did not match any fields in a record for {}; routing record to {}", new Object[] {coordinateKey, flowFile, rels});
                return rels;
            }

            if (lookupFieldValues.size() > 1) {
                final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
                getLogger().debug("RecordPath for property '{}' matched {} fields in a record for {}; routing record to {}",
                    new Object[] {coordinateKey, lookupFieldValues.size(), flowFile, rels});
                return rels;
            }

            final FieldValue fieldValue = lookupFieldValues.get(0);
            final Object coordinateValue = (fieldValue.getValue() instanceof Number || fieldValue.getValue() instanceof Boolean)
                    ? fieldValue.getValue() : DataTypeUtils.toString(fieldValue.getValue(), (String) null);
            lookupCoordinates.put(coordinateKey, coordinateValue);
        }

        final Optional<?> lookupValueOption;
        try {
            lookupValueOption = lookupService.lookup(lookupCoordinates, flowFile.getAttributes());
        } catch (final Exception e) {
            throw new ProcessException("Failed to lookup coordinates " + lookupCoordinates + " in Lookup Service", e);
        }

        if (!lookupValueOption.isPresent()) {
            final Set<Relationship> rels = routeToMatchedUnmatched ? UNMATCHED_COLLECTION : SUCCESS_COLLECTION;
            return rels;
        }

        // Ensure that the Record has the appropriate schema to account for the newly added values
        final RecordPath resultPath = flowFileContext.getValue();
        if (resultPath != null) {
            final Object lookupValue = lookupValueOption.get();
            final RecordPathResult resultPathResult = flowFileContext.getValue().evaluate(record);

            final String resultContentsValue = context.getProperty(RESULT_CONTENTS).getValue();
            if (RESULT_RECORD_FIELDS.getValue().equals(resultContentsValue) && lookupValue instanceof Record) {
                final Record lookupRecord = (Record) lookupValue;

                // User wants to add all fields of the resultant Record to the specified Record Path.
                // If the destination Record Path returns to us a Record, then we will add all field values of
                // the Lookup Record to the destination Record. However, if the destination Record Path returns
                // something other than a Record, then we can't add the fields to it. We can only replace it,
                // because it doesn't make sense to add fields to anything but a Record.
                resultPathResult.getSelectedFields().forEach(fieldVal -> {
                    final Object destinationValue = fieldVal.getValue();

                    if (destinationValue instanceof Record) {
                        final Record destinationRecord = (Record) destinationValue;

                        for (final String fieldName : lookupRecord.getRawFieldNames()) {
                            final Object value = lookupRecord.getValue(fieldName);

                            final Optional<RecordField> recordFieldOption = lookupRecord.getSchema().getField(fieldName);
                            if (recordFieldOption.isPresent()) {
                                // Even if the looked up field is not nullable, if the lookup key didn't match with any record,
                                // and matched/unmatched records are written to the same FlowFile routed to 'success' relationship,
                                // then enriched fields should be nullable to support unmatched records whose enriched fields will be null.
                                RecordField field = recordFieldOption.get();
                                if (!routeToMatchedUnmatched && !field.isNullable()) {
                                    field = new RecordField(field.getFieldName(), field.getDataType(), field.getDefaultValue(), field.getAliases(), true);
                                }
                                destinationRecord.setValue(field, value);
                            } else {
                                destinationRecord.setValue(fieldName, value);
                            }
                        }
                    } else {
                        final Optional<Record> parentOption = fieldVal.getParentRecord();
                        parentOption.ifPresent(parent -> parent.setValue(fieldVal.getField(), lookupRecord));
                    }
                });
            } else {
                final DataType inferredDataType = DataTypeUtils.inferDataType(lookupValue, RecordFieldType.STRING.getDataType());
                resultPathResult.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(lookupValue, inferredDataType));
            }

            record.incorporateInactiveFields();
        }

        final Set<Relationship> rels = routeToMatchedUnmatched ? MATCHED_COLLECTION : SUCCESS_COLLECTION;
        return rels;
    }

    @Override
    protected boolean isRouteOriginal() {
        return false;
    }

    @Override
    protected Tuple<Map<String, RecordPath>, RecordPath> getFlowFileContext(final FlowFile flowFile, final ProcessContext context) {
        final Map<String, RecordPath> recordPaths = new HashMap<>();
        for (final PropertyDescriptor prop : context.getProperties().keySet()) {
            if (!prop.isDynamic()) {
                continue;
            }

            final String pathText = context.getProperty(prop).evaluateAttributeExpressions(flowFile).getValue();
            final RecordPath lookupRecordPath = recordPathCache.getCompiled(pathText);
            recordPaths.put(prop.getName(), lookupRecordPath);
        }

        final RecordPath resultRecordPath;
        if (context.getProperty(RESULT_RECORD_PATH).isSet()) {
            final String resultPathText = context.getProperty(RESULT_RECORD_PATH).evaluateAttributeExpressions(flowFile).getValue();
            resultRecordPath = recordPathCache.getCompiled(resultPathText);
        } else {
            resultRecordPath = null;
        }

        return new Tuple<>(recordPaths, resultRecordPath);
    }

}
