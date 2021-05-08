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
package org.apache.nifi.processors.azure.storage;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processors.azure.storage.utils.ADLSFileInfo;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.nifi.processor.util.list.ListedEntityTracker.INITIAL_LISTING_TARGET;
import static org.apache.nifi.processor.util.list.ListedEntityTracker.TRACKING_STATE_CACHE;
import static org.apache.nifi.processor.util.list.ListedEntityTracker.TRACKING_TIME_WINDOW;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.ADLS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.DIRECTORY;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.FILESYSTEM;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.evaluateDirectoryProperty;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.evaluateFileSystemProperty;
import static org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor.getStorageClient;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_FILE_PATH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_LAST_MODIFIED;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_DESCRIPTION_LENGTH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_DIRECTORY;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_ETAG;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILENAME;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILESYSTEM;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_FILE_PATH;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LAST_MODIFIED;
import static org.apache.nifi.processors.azure.storage.utils.ADLSAttributes.ATTR_NAME_LENGTH;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({PutAzureDataLakeStorage.class, DeleteAzureDataLakeStorage.class, FetchAzureDataLakeStorage.class})
@CapabilityDescription("列出Azure Data Lake存储第2代文件系统中的目录")
@WritesAttributes({
        @WritesAttribute(attribute = ATTR_NAME_FILESYSTEM, description = ATTR_DESCRIPTION_FILESYSTEM),
        @WritesAttribute(attribute = ATTR_NAME_FILE_PATH, description = ATTR_DESCRIPTION_FILE_PATH),
        @WritesAttribute(attribute = ATTR_NAME_DIRECTORY, description = ATTR_DESCRIPTION_DIRECTORY),
        @WritesAttribute(attribute = ATTR_NAME_FILENAME, description = ATTR_DESCRIPTION_FILENAME),
        @WritesAttribute(attribute = ATTR_NAME_LENGTH, description = ATTR_DESCRIPTION_LENGTH),
        @WritesAttribute(attribute = ATTR_NAME_LAST_MODIFIED, description = ATTR_DESCRIPTION_LAST_MODIFIED),
        @WritesAttribute(attribute = ATTR_NAME_ETAG, description = ATTR_DESCRIPTION_ETAG)
})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. " +
        "This allows the Processor to list only files that have been added or modified after this date the next time that the Processor is run. State is " +
        "stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected, the new node can pick up " +
        "where the previous node left off, without duplicating the data.")
public class ListAzureDataLakeStorage extends AbstractListProcessor<ADLSFileInfo> {

    public static final PropertyDescriptor RECURSE_SUBDIRECTORIES = new PropertyDescriptor.Builder()
            .name("recurse-subdirectories")
            .displayName("Recurse Subdirectories")
            .description("Indicates whether to list files from subdirectories of the directory")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("file-filter")
            .displayName("File Filter")
            .description("Only files whose names match the given regular expression will be listed")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_WITH_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PATH_FILTER = new PropertyDescriptor.Builder()
            .name("path-filter")
            .displayName("Path Filter")
            .description(String.format("When '%s' is true, then only subdirectories whose paths match the given regular expression will be scanned", RECURSE_SUBDIRECTORIES.getDisplayName()))
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_WITH_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            RECURSE_SUBDIRECTORIES,
            FILE_FILTER,
            PATH_FILTER,
            RECORD_WRITER,
            LISTING_STRATEGY,
            TRACKING_STATE_CACHE,
            TRACKING_TIME_WINDOW,
            INITIAL_LISTING_TARGET));

    private static final Set<PropertyDescriptor> LISTING_RESET_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            ADLS_CREDENTIALS_SERVICE,
            FILESYSTEM,
            DIRECTORY,
            RECURSE_SUBDIRECTORIES,
            FILE_FILTER,
            PATH_FILTER,
            LISTING_STRATEGY)));

    private volatile Pattern filePattern;
    private volatile Pattern pathPattern;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        String fileFilter = context.getProperty(FILE_FILTER).evaluateAttributeExpressions().getValue();
        filePattern = fileFilter != null ? Pattern.compile(fileFilter) : null;

        String pathFilter = context.getProperty(PATH_FILTER).evaluateAttributeExpressions().getValue();
        pathPattern = pathFilter != null ? Pattern.compile(pathFilter) : null;
    }

    @OnStopped
    public void onStopped() {
        filePattern = null;
        pathPattern = null;
    }

    @Override
    protected void customValidate(ValidationContext context, Collection<ValidationResult> results) {
        if (context.getProperty(PATH_FILTER).isSet() && !context.getProperty(RECURSE_SUBDIRECTORIES).asBoolean()) {
            results.add(new ValidationResult.Builder()
                    .subject(PATH_FILTER.getDisplayName())
                    .valid(false)
                    .explanation(String.format("'%s' cannot be set when '%s' is false", PATH_FILTER.getDisplayName(), RECURSE_SUBDIRECTORIES.getDisplayName()))
                    .build());
        }
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return ADLSFileInfo.getRecordSchema();
    }

    @Override
    protected Scope getStateScope(PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected String getDefaultTimePrecision() {
        return PRECISION_MILLIS.getValue();
    }

    @Override
    protected boolean isListingResetNecessary(PropertyDescriptor property) {
        return LISTING_RESET_PROPERTIES.contains(property);
    }

    @Override
    protected String getPath(ProcessContext context) {
        String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue();
        return directory != null ? directory : ".";
    }

    @Override
    protected List<ADLSFileInfo> performListing(ProcessContext context, Long minTimestamp) throws IOException {
        try {
            String fileSystem = evaluateFileSystemProperty(context, null);
            String baseDirectory = evaluateDirectoryProperty(context, null);
            boolean recurseSubdirectories = context.getProperty(RECURSE_SUBDIRECTORIES).asBoolean();

            DataLakeServiceClient storageClient = getStorageClient(context, null);
            DataLakeFileSystemClient fileSystemClient = storageClient.getFileSystemClient(fileSystem);

            ListPathsOptions options = new ListPathsOptions();
            options.setPath(baseDirectory);
            options.setRecursive(recurseSubdirectories);

            Pattern baseDirectoryPattern = Pattern.compile("^" + baseDirectory + "/?");

            List<ADLSFileInfo> listing = fileSystemClient.listPaths(options, null).stream()
                    .filter(pathItem -> !pathItem.isDirectory())
                    .map(pathItem -> new ADLSFileInfo.Builder()
                            .fileSystem(fileSystem)
                            .filePath(pathItem.getName())
                            .length(pathItem.getContentLength())
                            .lastModified(pathItem.getLastModified().toInstant().toEpochMilli())
                            .etag(pathItem.getETag())
                            .build())
                    .filter(fileInfo -> filePattern == null || filePattern.matcher(fileInfo.getFilename()).matches())
                    .filter(fileInfo -> pathPattern == null || pathPattern.matcher(RegExUtils.removeFirst(fileInfo.getDirectory(), baseDirectoryPattern)).matches())
                    .collect(Collectors.toList());

            return listing;
        } catch (Exception e) {
            getLogger().error("Failed to list directory on Azure Data Lake Storage", e);
            throw new IOException(ExceptionUtils.getRootCause(e));
        }
    }

    @Override
    protected Map<String, String> createAttributes(ADLSFileInfo fileInfo, ProcessContext context) {
        Map<String, String> attributes = new HashMap<>();

        attributes.put(ATTR_NAME_FILESYSTEM, fileInfo.getFileSystem());
        attributes.put(ATTR_NAME_FILE_PATH, fileInfo.getFilePath());
        attributes.put(ATTR_NAME_DIRECTORY, fileInfo.getDirectory());
        attributes.put(ATTR_NAME_FILENAME, fileInfo.getFilename());
        attributes.put(ATTR_NAME_LENGTH, String.valueOf(fileInfo.getLength()));
        attributes.put(ATTR_NAME_LAST_MODIFIED, String.valueOf(fileInfo.getLastModified()));
        attributes.put(ATTR_NAME_ETAG, fileInfo.getEtag());

        return attributes;
    }
}