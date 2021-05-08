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

package org.apache.nifi.schema.access;

import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HortonworksAttributeSchemaReferenceStrategy implements SchemaAccessStrategy {
    private final Set<SchemaField> schemaFields;

    public static final String SCHEMA_ID_ATTRIBUTE = "schema.identifier";
    public static final String SCHEMA_VERSION_ATTRIBUTE = "schema.version";
    public static final String SCHEMA_PROTOCOL_VERSION_ATTRIBUTE = "schema.protocol.version";
    public static final String SCHEMA_VERSION_ID_ATTRIBUTE = "schema.version.id";

    private final SchemaRegistry schemaRegistry;

    public HortonworksAttributeSchemaReferenceStrategy(final SchemaRegistry schemaRegistry) {
        this.schemaRegistry = schemaRegistry;

        schemaFields = new HashSet<>();
        schemaFields.add(SchemaField.SCHEMA_IDENTIFIER);
        schemaFields.add(SchemaField.SCHEMA_VERSION);
        schemaFields.add(SchemaField.SCHEMA_VERSION_ID);
        schemaFields.addAll(schemaRegistry == null ? Collections.emptySet() : schemaRegistry.getSuppliedSchemaFields());
    }

    public boolean isFlowFileRequired() {
        return true;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        final String schemaProtocol = variables.get(SCHEMA_PROTOCOL_VERSION_ATTRIBUTE);
        if (!isNumber(schemaProtocol)) {
            throw new SchemaNotFoundException("Could not determine Schema for " + variables + " because the " + SCHEMA_PROTOCOL_VERSION_ATTRIBUTE + " has a value of '"
                + schemaProtocol + "', which is not a valid Protocol Version number");
        }

        final int protocol = Integer.parseInt(schemaProtocol);
        if (protocol < HortonworksProtocolVersions.MIN_VERSION || protocol > HortonworksProtocolVersions.MAX_VERSION) {
            throw new SchemaNotFoundException("Could not determine Schema for " + variables + " because the " + SCHEMA_PROTOCOL_VERSION_ATTRIBUTE + " has a value of '"
                    + schemaProtocol + "', which is not a valid Protocol Version number. Expected Protocol Version to be a value between "
                    + HortonworksProtocolVersions.MIN_VERSION + " and " + HortonworksProtocolVersions.MAX_VERSION + ".");
        }

        SchemaIdentifier identifier;

        switch (protocol) {
            case 1:
                final String schemaIdentifier = variables.get(SCHEMA_ID_ATTRIBUTE);
                if (!isNumber(schemaIdentifier)) {
                    throw new SchemaNotFoundException("Could not determine Schema because " + SCHEMA_ID_ATTRIBUTE + " has a value of '"
                            + schemaIdentifier + "', which is not a valid Schema Identifier and is required by Protocol Version " + protocol);
                }

                final String schemaVersion = variables.get(SCHEMA_VERSION_ATTRIBUTE);
                if (!isNumber(schemaVersion)) {
                    throw new SchemaNotFoundException("Could not determine Schema because " + SCHEMA_VERSION_ATTRIBUTE + " has a value of '"
                            + schemaVersion + "', which is not a valid Schema Version and is required by Protocol Version " + protocol);
                }

                final long schemaId = Long.parseLong(schemaIdentifier);
                final int version = Integer.parseInt(schemaVersion);
                identifier = SchemaIdentifier.builder().id(schemaId).version(version).build();
                break;
            case 2:
            case 3:
                final String schemaVersionId = variables.get(SCHEMA_VERSION_ID_ATTRIBUTE);
                if (!isNumber(schemaVersionId)) {
                    throw new SchemaNotFoundException("Could not determine schema because " + SCHEMA_VERSION_ID_ATTRIBUTE + " has a value of '"
                            + schemaVersionId + "', which is not a valid Schema Version Identifier and is required by Protocol Version " + protocol);
                }

                final long svi = Long.parseLong(schemaVersionId);
                identifier = SchemaIdentifier.builder().schemaVersionId(svi).build();
                break;
            default:
                throw new SchemaNotFoundException("Unknown Protocol Version: " + protocol);
        }

        final RecordSchema schema = schemaRegistry.retrieveSchema(identifier);
        if (schema == null) {
            throw new SchemaNotFoundException("Could not find a Schema in the Schema Registry with Schema Identifier '" + identifier.toString() + "'");
        }

        return schema;
    }

    private static boolean isNumber(final String value) {
        if (value == null) {
            return false;
        }

        final String trimmed = value.trim();
        if (value.isEmpty()) {
            return false;
        }

        for (int i = 0; i < trimmed.length(); i++) {
            final char c = value.charAt(i);
            if (c > '9' || c < '0') {
                return false;
            }
        }

        return true;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
