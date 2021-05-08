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
package org.apache.nifi.schemaregistry.services;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.util.Set;

/**
 * Represents {@link ControllerService} strategy to expose internal and/or
 * integrate with external Schema Registry
 */
public interface SchemaRegistry extends ControllerService {

    /**
     * @deprecated Use {@link #retrieveSchema(SchemaIdentifier)} instead
     *
     * Retrieves and returns the textual representation of the schema based on
     * the provided name of the schema available in Schema Registry.
     *
     * @return the text that corresponds to the latest version of the schema with the given name
     *
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema with the given name
     */
    default String retrieveSchemaText(String schemaName) throws IOException, SchemaNotFoundException {
        final RecordSchema recordSchema = retrieveSchema(SchemaIdentifier.builder().name(schemaName).build());
        if (recordSchema == null) {
            throw new SchemaNotFoundException("Could not find schema with name '" + schemaName + "'");
        }
        return recordSchema.getSchemaText().get();
    }

    /**
     * @deprecated Use {@link #retrieveSchema(SchemaIdentifier)} instead
     *
     * Retrieves the textual representation of the schema with the given ID and version
     *
     * @param schemaId the unique identifier for the desired schema
     * @param version the version of the desired schema
     * @return the textual representation of the schema with the given ID and version
     *
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema with the given id and version
     */
    default String retrieveSchemaText(long schemaId, int version) throws IOException, SchemaNotFoundException {
        final RecordSchema recordSchema = retrieveSchema(SchemaIdentifier.builder().id(schemaId).version(version).build());
        if (recordSchema == null) {
            throw new SchemaNotFoundException("Could not find schema with ID '" + schemaId + "' and version '" + version + "'");
        }
        return recordSchema.getSchemaText().get();
    }

    /**
     * @deprecated Use {@link #retrieveSchema(SchemaIdentifier)} instead
     *
     * Retrieves and returns the RecordSchema based on the provided name of the schema available in Schema Registry. The RecordSchema
     * that is returned must have the Schema's name populated in its SchemaIdentifier. I.e., a call to
     * {@link RecordSchema}.{@link RecordSchema#getIdentifier() getIdentifier()}.{@link SchemaIdentifier#getName() getName()}
     * will always return an {@link java.util.Optional} that is not empty.
     *
     * @return the latest version of the schema with the given name, or <code>null</code> if no schema can be found with the given name.
     * @throws SchemaNotFoundException if unable to find the schema with the given name
     */
    default RecordSchema retrieveSchema(String schemaName) throws IOException, SchemaNotFoundException {
        return retrieveSchema(SchemaIdentifier.builder().name(schemaName).build());
    }


    /**
     * @deprecated Use {@link #retrieveSchema(SchemaIdentifier)} instead
     *
     * Retrieves the schema with the given ID and version. The RecordSchema that is returned must have the Schema's identifier and version
     * populated in its SchemaIdentifier. I.e., a call to
     * {@link RecordSchema}.{@link RecordSchema#getIdentifier() getIdentifier()}.{@link SchemaIdentifier#getIdentifier() getIdentifier()}
     * will always return an {@link java.util.Optional} that is not empty, as will a call to
     * {@link RecordSchema}.{@link RecordSchema#getIdentifier() getIdentifier()}.{@link SchemaIdentifier#getVersion() getVersion()}.
     *
     * @param schemaId the unique identifier for the desired schema
     * @param version the version of the desired schema
     * @return the schema with the given ID and version or <code>null</code> if no schema
     *         can be found with the given ID and version
     *
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema with the given id and version
     */
    default RecordSchema retrieveSchema(long schemaId, int version) throws IOException, SchemaNotFoundException {
        return retrieveSchema(SchemaIdentifier.builder().id(schemaId).version(version).build());
    }

    /**
     * Retrieves the schema based on the provided descriptor. The descriptor must contain and schemaIdentifier or name, but not both, along
     * with a version, and an optional branch name. For implementations that do not support branching, the branch name will be ignored.
     *
     * @param schemaIdentifier a schema schemaIdentifier
     * @return the schema for the given descriptor
     * @throws IOException if unable to communicate with the backing store
     * @throws SchemaNotFoundException if unable to find the schema based on the given descriptor
     */
    RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException;

    /**
     * @return the set of all Schema Fields that are supplied by the RecordSchema that is returned from {@link #retrieveSchema(SchemaIdentifier)}
     */
    Set<SchemaField> getSuppliedSchemaFields();
}
