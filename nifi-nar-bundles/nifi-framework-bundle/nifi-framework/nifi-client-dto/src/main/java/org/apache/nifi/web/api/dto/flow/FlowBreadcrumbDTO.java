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
package org.apache.nifi.web.api.dto.flow;

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;

import javax.xml.bind.annotation.XmlType;

/**
 * Breadcrumb for the flow.
 */
@XmlType(name = "flowBreadcrumb")
public class FlowBreadcrumbDTO {

    private String id;
    private String name;
    private VersionControlInformationDTO versionControlInformation;

    /**
     * The id for this group.
     *
     * @return The id
     */
    @ApiModelProperty(
        value = "The id of the group."
    )
    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The name for this group.
     *
     * @return The name
     */
    @ApiModelProperty(
        value = "The id of the group."
    )
    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    /**
     * @return the process group version control information or null if not version controlled
     */
    @ApiModelProperty(
            value = "The process group version control information or null if not version controlled."
    )
    public VersionControlInformationDTO getVersionControlInformation() {
        return versionControlInformation;
    }

    public void setVersionControlInformation(VersionControlInformationDTO versionControlInformation) {
        this.versionControlInformation = versionControlInformation;
    }
}
