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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.standard.util.FTPTransfer;
import org.apache.nifi.processors.standard.util.FileInfo;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.SFTPTransfer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"list", "sftp", "remote", "ingest", "source", "input", "files"})
@CapabilityDescription("列出SFTP服务器上的文件。对于远程服务器上找到的每个文件，将创建一个新的流文件，并将filename属性设置为远程服务器上的文件名称。这可以与FetchSFTP一起使用来获取这些文件。")
@SeeAlso({FetchSFTP.class, GetSFTP.class, PutSFTP.class})
@WritesAttributes({
    @WritesAttribute(attribute = "sftp.remote.host", description = "The hostname of the SFTP Server"),
    @WritesAttribute(attribute = "sftp.remote.port", description = "The port that was connected to on the SFTP Server"),
    @WritesAttribute(attribute = "sftp.listing.user", description = "The username of the user that performed the SFTP Listing"),
    @WritesAttribute(attribute = ListFile.FILE_OWNER_ATTRIBUTE, description = "The numeric owner id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_GROUP_ATTRIBUTE, description = "The numeric group id of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_PERMISSIONS_ATTRIBUTE, description = "The read/write/execute permissions of the source file"),
    @WritesAttribute(attribute = ListFile.FILE_SIZE_ATTRIBUTE, description = "The number of bytes in the source file"),
    @WritesAttribute(attribute = ListFile.FILE_LAST_MODIFY_TIME_ATTRIBUTE, description = "The timestamp of when the file in the filesystem was" +
                  "last modified as 'yyyy-MM-dd'T'HH:mm:ssZ'"),
    @WritesAttribute(attribute = "filename", description = "The name of the file on the SFTP Server"),
    @WritesAttribute(attribute = "path", description = "The fully qualified name of the directory on the SFTP Server from which the file was pulled"),
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of files, the timestamp of the newest file is stored. "
    + "This allows the Processor to list only files that have been added or modified after "
    + "this date the next time that the Processor is run. State is stored across the cluster so that this Processor can be run on Primary Node only and if "
    + "a new Primary Node is selected, the new node will not duplicate the data that was listed by the previous Primary Node.")
public class ListSFTP extends ListFileTransfer {

    private volatile Predicate<FileInfo> fileFilter;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final PropertyDescriptor port = new PropertyDescriptor.Builder().fromPropertyDescriptor(UNDEFAULTED_PORT).defaultValue("22").build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FILE_TRANSFER_LISTING_STRATEGY);
        properties.add(HOSTNAME);
        properties.add(port);
        properties.add(USERNAME);
        properties.add(SFTPTransfer.PASSWORD);
        properties.add(SFTPTransfer.PRIVATE_KEY_PATH);
        properties.add(SFTPTransfer.PRIVATE_KEY_PASSPHRASE);
        properties.add(REMOTE_PATH);
        properties.add(RECORD_WRITER);
        properties.add(DISTRIBUTED_CACHE_SERVICE);
        properties.add(SFTPTransfer.RECURSIVE_SEARCH);
        properties.add(SFTPTransfer.FOLLOW_SYMLINK);
        properties.add(SFTPTransfer.FILE_FILTER_REGEX);
        properties.add(SFTPTransfer.PATH_FILTER_REGEX);
        properties.add(SFTPTransfer.IGNORE_DOTTED_FILES);
        properties.add(SFTPTransfer.STRICT_HOST_KEY_CHECKING);
        properties.add(SFTPTransfer.HOST_KEY_FILE);
        properties.add(SFTPTransfer.CONNECTION_TIMEOUT);
        properties.add(SFTPTransfer.DATA_TIMEOUT);
        properties.add(SFTPTransfer.USE_KEEPALIVE_ON_TIMEOUT);
        properties.add(TARGET_SYSTEM_TIMESTAMP_PRECISION);
        properties.add(SFTPTransfer.PROXY_CONFIGURATION_SERVICE);
        properties.add(FTPTransfer.PROXY_TYPE);
        properties.add(FTPTransfer.PROXY_HOST);
        properties.add(FTPTransfer.PROXY_PORT);
        properties.add(FTPTransfer.HTTP_PROXY_USERNAME);
        properties.add(FTPTransfer.HTTP_PROXY_PASSWORD);
        properties.add(ListedEntityTracker.TRACKING_STATE_CACHE);
        properties.add(ListedEntityTracker.TRACKING_TIME_WINDOW);
        properties.add(ListedEntityTracker.INITIAL_LISTING_TARGET);
        properties.add(ListFile.MIN_AGE);
        properties.add(ListFile.MAX_AGE);
        properties.add(ListFile.MIN_SIZE);
        properties.add(ListFile.MAX_SIZE);
        return properties;
    }

    @Override
    protected FileTransfer getFileTransfer(final ProcessContext context) {
        return new SFTPTransfer(context, getLogger());
    }

    @Override
    protected String getProtocolName() {
        return "sftp";
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        // Use cluster scope so that component can be run on Primary Node Only and can still
        // pick up where it left off, even if the Primary Node changes.
        return Scope.CLUSTER;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        SFTPTransfer.validateProxySpec(validationContext, results);
    }

    @Override
    protected List<FileInfo> performListing(final ProcessContext context, final Long minTimestamp) throws IOException {
        final List<FileInfo> listing = super.performListing(context, minTimestamp);

        return listing.stream()
                .filter(fileFilter)
                .collect(Collectors.toList());
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileFilter = createFileFilter(context);
    }

    private Predicate<FileInfo> createFileFilter(final ProcessContext context) {
        final long minSize = context.getProperty(ListFile.MIN_SIZE).asDataSize(DataUnit.B).longValue();
        final Double maxSize = context.getProperty(ListFile.MAX_SIZE).asDataSize(DataUnit.B);
        final long minAge = context.getProperty(ListFile.MIN_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
        final Long maxAge = context.getProperty(ListFile.MAX_AGE).asTimePeriod(TimeUnit.MILLISECONDS);

        return (attributes) -> {
            if(attributes.isDirectory()) {
                return true;
            }

            if (minSize > attributes.getSize()) {
                return false;
            }
            if (maxSize != null && maxSize < attributes.getSize()) {
                return false;
            }
            final long fileAge = System.currentTimeMillis() - attributes.getLastModifiedTime();
            if (minAge > fileAge) {
                return false;
            }
            if (maxAge != null && maxAge < fileAge) {
                return false;
            }

            return true;
        };
    }
}
