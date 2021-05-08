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

package org.apache.nifi.web.api.entity;

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.registry.flow.VersionedFlowSnapshotMetadata;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "versionedFlowSnapshotMetadataEntity")
public class VersionedFlowSnapshotMetadataEntity extends Entity {
    private VersionedFlowSnapshotMetadata versionedFlowSnapshotMetadata;
    private String registryId;

    @ApiModelProperty("The collection of versioned flow snapshot metadata")
    public VersionedFlowSnapshotMetadata getVersionedFlowSnapshotMetadata() {
        return versionedFlowSnapshotMetadata;
    }

    public void setVersionedFlowMetadata(VersionedFlowSnapshotMetadata versionedFlowSnapshotMetadata) {
        this.versionedFlowSnapshotMetadata = versionedFlowSnapshotMetadata;
    }

    @ApiModelProperty("The ID of the Registry that this flow belongs to")
    public String getRegistryId() {
        return registryId;
    }

    public void setRegistryId(String registryId) {
        this.registryId = registryId;
    }
}
