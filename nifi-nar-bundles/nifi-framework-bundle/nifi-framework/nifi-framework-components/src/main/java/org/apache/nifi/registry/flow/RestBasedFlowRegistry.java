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

package org.apache.nifi.registry.flow;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.BucketClient;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RestBasedFlowRegistry implements FlowRegistry {
    public static final String FLOW_ENCODING_VERSION = "1.0";

    private final FlowRegistryClient flowRegistryClient;
    private final String identifier;
    private final SSLContext sslContext;
    private volatile String description;
    private volatile String url;
    private volatile String name;

    private NiFiRegistryClient registryClient;

    public RestBasedFlowRegistry(final FlowRegistryClient flowRegistryClient, final String identifier, final String url, final SSLContext sslContext, final String name) {
        this.flowRegistryClient = flowRegistryClient;
        this.identifier = identifier;
        this.url = url;
        this.name = name;
        this.sslContext = sslContext;
    }

    private synchronized NiFiRegistryClient getRegistryClient() {
        if (registryClient != null) {
            return registryClient;
        }

        final NiFiRegistryClientConfig config = new NiFiRegistryClientConfig.Builder()
            .connectTimeout(30000)
            .readTimeout(30000)
            .sslContext(sslContext)
            .baseUrl(url)
            .build();

        registryClient = new JerseyNiFiRegistryClient.Builder()
            .config(config)
            .build();

        return registryClient;
    }

    private synchronized void invalidateClient() {
        this.registryClient = null;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(final String description) {
        this.description = description;
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public synchronized void setURL(final String url) {
        this.url = url;
        invalidateClient();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    private String getIdentity(final NiFiUser user) {
        return (user == null || user.isAnonymous()) ? null : user.getIdentity();
    }

    private BucketClient getBucketClient(final NiFiUser user) {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final BucketClient bucketClient = identity == null ? registryClient.getBucketClient() : registryClient.getBucketClient(identity);
        return bucketClient;
    }

    private FlowSnapshotClient getFlowSnapshotClient(final NiFiUser user) {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final FlowSnapshotClient snapshotClient = identity == null ? registryClient.getFlowSnapshotClient() : registryClient.getFlowSnapshotClient(identity);
        return snapshotClient;
    }

    private FlowClient getFlowClient(final NiFiUser user) {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final FlowClient flowClient = identity == null ? registryClient.getFlowClient() : registryClient.getFlowClient(identity);
        return flowClient;
    }

    @Override
    public Set<Bucket> getBuckets(final NiFiUser user) throws IOException, NiFiRegistryException {
        final BucketClient bucketClient = getBucketClient(user);
        return new HashSet<>(bucketClient.getAll());
    }

    @Override
    public Bucket getBucket(final String bucketId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final BucketClient bucketClient = getBucketClient(user);
        return bucketClient.get(bucketId);
    }


    @Override
    public Set<VersionedFlow> getFlows(final String bucketId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getFlowClient(user);
        return new HashSet<>(flowClient.getByBucket(bucketId));
    }

    @Override
    public Set<VersionedFlowSnapshotMetadata> getFlowVersions(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(user);
        return new HashSet<>(snapshotClient.getSnapshotMetadata(bucketId, flowId));
    }

    @Override
    public VersionedFlow registerVersionedFlow(final VersionedFlow flow, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getFlowClient(user);
        return flowClient.create(flow);
    }

    @Override
    public VersionedFlow deleteVersionedFlow(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getFlowClient(user);
        return flowClient.delete(bucketId, flowId);
    }

    @Override
    public VersionedFlowSnapshot registerVersionedFlowSnapshot(final VersionedFlow flow, final VersionedProcessGroup snapshot,
                                                               final Map<String, ExternalControllerServiceReference> externalControllerServices,
                                                               final Map<String, VersionedParameterContext> parameterContexts, final String comments,
                                                               final int expectedVersion, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(user);
        final VersionedFlowSnapshot versionedFlowSnapshot = new VersionedFlowSnapshot();
        versionedFlowSnapshot.setFlowContents(snapshot);
        versionedFlowSnapshot.setExternalControllerServices(externalControllerServices);
        versionedFlowSnapshot.setParameterContexts(parameterContexts);
        versionedFlowSnapshot.setFlowEncodingVersion(FLOW_ENCODING_VERSION);

        final VersionedFlowSnapshotMetadata metadata = new VersionedFlowSnapshotMetadata();
        metadata.setBucketIdentifier(flow.getBucketIdentifier());
        metadata.setFlowIdentifier(flow.getIdentifier());
        metadata.setAuthor(getIdentity(user));
        metadata.setTimestamp(System.currentTimeMillis());
        metadata.setVersion(expectedVersion);
        metadata.setComments(comments);

        versionedFlowSnapshot.setSnapshotMetadata(metadata);
        return snapshotClient.create(versionedFlowSnapshot);
    }

    @Override
    public int getLatestVersion(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        return (int) getFlowClient(user).get(bucketId, flowId).getVersionCount();
    }

    @Override
    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows, final NiFiUser user)
            throws IOException, NiFiRegistryException {

        final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(user);
        final VersionedFlowSnapshot flowSnapshot = snapshotClient.get(bucketId, flowId, version);

        if (fetchRemoteFlows) {
            final VersionedProcessGroup contents = flowSnapshot.getFlowContents();
            for (final VersionedProcessGroup child : contents.getProcessGroups()) {
                populateVersionedContentsRecursively(child, user);
            }
        }

        return flowSnapshot;
    }

    @Override
    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows) throws IOException, NiFiRegistryException {
        return getFlowContents(bucketId, flowId, version, fetchRemoteFlows, null);
    }

    private void populateVersionedContentsRecursively(final VersionedProcessGroup group, final NiFiUser user) throws NiFiRegistryException, IOException {
        if (group == null) {
            return;
        }

        final VersionedFlowCoordinates coordinates = group.getVersionedFlowCoordinates();
        if (coordinates != null) {
            final String registryUrl = coordinates.getRegistryUrl();
            final String bucketId = coordinates.getBucketId();
            final String flowId = coordinates.getFlowId();
            final int version = coordinates.getVersion();

            final String registryId = flowRegistryClient.getFlowRegistryId(registryUrl);
            if (registryId == null) {
                throw new NiFiRegistryException("Flow contains a reference to another Versioned Flow located at URL " + registryUrl
                    + " but NiFi is not configured to communicate with a Flow Registry at that URL");
            }

            final FlowRegistry flowRegistry = flowRegistryClient.getFlowRegistry(registryId);
            final VersionedFlowSnapshot snapshot = flowRegistry.getFlowContents(bucketId, flowId, version, true, user);
            final VersionedProcessGroup contents = snapshot.getFlowContents();

            group.setComments(contents.getComments());
            group.setConnections(contents.getConnections());
            group.setControllerServices(contents.getControllerServices());
            group.setFunnels(contents.getFunnels());
            group.setInputPorts(contents.getInputPorts());
            group.setLabels(contents.getLabels());
            group.setOutputPorts(contents.getOutputPorts());
            group.setProcessGroups(contents.getProcessGroups());
            group.setProcessors(contents.getProcessors());
            group.setRemoteProcessGroups(contents.getRemoteProcessGroups());
            group.setVariables(contents.getVariables());
            group.setParameterContextName(contents.getParameterContextName());
            group.setFlowFileConcurrency(contents.getFlowFileConcurrency());
            group.setFlowFileOutboundPolicy(contents.getFlowFileOutboundPolicy());
            coordinates.setLatest(snapshot.isLatest());
        }

        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            populateVersionedContentsRecursively(child, user);
        }
    }

    @Override
    public VersionedFlow getVersionedFlow(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getFlowClient(user);
        return flowClient.get(bucketId, flowId);
    }

    @Override
    public VersionedFlow getVersionedFlow(final String bucketId, final String flowId) throws IOException, NiFiRegistryException {
        final FlowClient flowClient = getRegistryClient().getFlowClient();
        return flowClient.get(bucketId, flowId);
    }
}
