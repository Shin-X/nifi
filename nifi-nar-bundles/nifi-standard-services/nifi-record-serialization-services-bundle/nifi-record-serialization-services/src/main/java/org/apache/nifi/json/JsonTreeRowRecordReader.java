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

package org.apache.nifi.json;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SerializedForm;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class JsonTreeRowRecordReader extends AbstractJsonRowRecordReader {
    private final RecordSchema schema;


    public JsonTreeRowRecordReader(final InputStream in, final ComponentLog logger, final RecordSchema schema,
        final String dateFormat, final String timeFormat, final String timestampFormat) throws IOException, MalformedRecordException {
        super(in, logger, dateFormat, timeFormat, timestampFormat);
        this.schema = schema;
    }



    @Override
    protected Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final boolean coerceTypes, final boolean dropUnknownFields)
        throws IOException, MalformedRecordException {
        return convertJsonNodeToRecord(jsonNode, schema, coerceTypes, dropUnknownFields, null);
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final boolean coerceTypes, final boolean dropUnknown, final String fieldNamePrefix)
        throws IOException, MalformedRecordException {
        if (jsonNode == null) {
            return null;
        }

        return convertJsonNodeToRecord(jsonNode, schema, fieldNamePrefix, coerceTypes, dropUnknown);
    }

    private JsonNode getChildNode(final JsonNode jsonNode, final RecordField field) {
        if (jsonNode.has(field.getFieldName())) {
            return jsonNode.get(field.getFieldName());
        }

        for (final String alias : field.getAliases()) {
            if (jsonNode.has(alias)) {
                return jsonNode.get(alias);
            }
        }

        return null;
    }

    private Record convertJsonNodeToRecord(final JsonNode jsonNode, final RecordSchema schema, final String fieldNamePrefix,
            final boolean coerceTypes, final boolean dropUnknown) throws IOException, MalformedRecordException {

        final Map<String, Object> values = new HashMap<>(schema.getFieldCount() * 2);

        if (dropUnknown) {
            for (final RecordField recordField : schema.getFields()) {
                final JsonNode childNode = getChildNode(jsonNode, recordField);
                if (childNode == null) {
                    continue;
                }

                final String fieldName = recordField.getFieldName();

                Object value;
                if (coerceTypes) {
                    final DataType desiredType = recordField.getDataType();
                    final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                    value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                } else {
                    value = getRawNodeValue(childNode, recordField == null ? null : recordField.getDataType(), fieldName);
                }

                values.put(fieldName, value);
            }
        } else {
            final Iterator<String> fieldNames = jsonNode.getFieldNames();
            while (fieldNames.hasNext()) {
                final String fieldName = fieldNames.next();
                final JsonNode childNode = jsonNode.get(fieldName);

                final RecordField recordField = schema.getField(fieldName).orElse(null);

                final Object value;
                if (coerceTypes && recordField != null) {
                    final DataType desiredType = recordField.getDataType();
                    final String fullFieldName = fieldNamePrefix == null ? fieldName : fieldNamePrefix + fieldName;
                    value = convertField(childNode, fullFieldName, desiredType, dropUnknown);
                } else {
                    value = getRawNodeValue(childNode, recordField == null ? null : recordField.getDataType(), fieldName);
                }

                values.put(fieldName, value);
            }
        }

        final Supplier<String> supplier = jsonNode::toString;
        return new MapRecord(schema, values, SerializedForm.of(supplier, "application/json"), false, dropUnknown);
    }


    protected Object convertField(final JsonNode fieldNode, final String fieldName, final DataType desiredType, final boolean dropUnknown) throws IOException, MalformedRecordException {
        if (fieldNode == null || fieldNode.isNull()) {
            return null;
        }

        switch (desiredType.getFieldType()) {
            case BOOLEAN:
            case BYTE:
            case CHAR:
            case DECIMAL:
            case DOUBLE:
            case FLOAT:
            case INT:
            case BIGINT:
            case LONG:
            case SHORT:
            case STRING:
            case ENUM:
            case DATE:
            case TIME:
            case TIMESTAMP: {
                final Object rawValue = getRawNodeValue(fieldNode, fieldName);
                final Object converted = DataTypeUtils.convertType(rawValue, desiredType, getLazyDateFormat(), getLazyTimeFormat(), getLazyTimestampFormat(), fieldName);
                return converted;
            }
            case MAP: {
                final DataType valueType = ((MapDataType) desiredType).getValueType();

                final Map<String, Object> map = new HashMap<>();
                final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
                while (fieldNameItr.hasNext()) {
                    final String childName = fieldNameItr.next();
                    final JsonNode childNode = fieldNode.get(childName);
                    final Object childValue = convertField(childNode, fieldName, valueType, dropUnknown);
                    map.put(childName, childValue);
                }

                return map;
            }
            case ARRAY: {
                final ArrayNode arrayNode = (ArrayNode) fieldNode;
                final int numElements = arrayNode.size();
                final Object[] arrayElements = new Object[numElements];
                int count = 0;
                for (final JsonNode node : arrayNode) {
                    final DataType elementType = ((ArrayDataType) desiredType).getElementType();
                    final Object converted = convertField(node, fieldName, elementType, dropUnknown);
                    arrayElements[count++] = converted;
                }

                return arrayElements;
            }
            case RECORD: {
                if (fieldNode.isObject()) {
                    RecordSchema childSchema;
                    if (desiredType instanceof RecordDataType) {
                        childSchema = ((RecordDataType) desiredType).getChildSchema();
                    } else {
                        return null;
                    }

                    if (childSchema == null) {
                        final List<RecordField> fields = new ArrayList<>();
                        final Iterator<String> fieldNameItr = fieldNode.getFieldNames();
                        while (fieldNameItr.hasNext()) {
                            fields.add(new RecordField(fieldNameItr.next(), RecordFieldType.STRING.getDataType()));
                        }

                        childSchema = new SimpleRecordSchema(fields);
                    }

                    return convertJsonNodeToRecord(fieldNode, childSchema, fieldName + ".", true, dropUnknown);
                } else {
                    return null;
                }
            }
            case CHOICE: {
                return DataTypeUtils.convertType(getRawNodeValue(fieldNode, desiredType, fieldName), desiredType, fieldName);
            }
        }

        return null;
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }
}
