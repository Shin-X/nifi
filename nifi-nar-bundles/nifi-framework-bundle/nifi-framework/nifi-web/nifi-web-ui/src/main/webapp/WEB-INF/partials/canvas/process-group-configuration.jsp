<%--
 Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>

<%@ page contentType="text/html;charste=UTF-8" pageEncoding="UTF-8" session="false" %>
<div id="process-group-configuration">
    <div id="process-group-configuration-header-text" class="settings-header-text">Process Group Configuration</div>
    <div class="settings-container">
        <div>
            <div id="process-group-configuration-tabs" class="settings-tabs tab-container"></div>
            <div class="clear"></div>
        </div>
        <div id="process-group-configuration-tabs-content">
            <button id="add-process-group-configuration-controller-service" class="add-button fa fa-plus" title="Create a new controller service"></button>
            <div id="general-process-group-configuration-tab-content" class="configuration-tab">
                <div id="general-process-group-configuration">
                    <div class="setting">
                        <div class="setting-name"><%--Process group name--%>进程组名称</div>
                        <span id="process-group-id" class="hidden"></span>
                        <div class="editable setting-field">
                            <input type="text" id="process-group-name" class="setting-input"/>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-name" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><%--Process group parameter context--%>进程组参数上下文</div>
                        <div class="editable setting-field">
                            <div id="process-group-parameter-context-combo"></div>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-parameter-context" class="unset">Unauthorized</span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><%--Process group comments--%>进程组注释</div>
                        <div class="editable setting-field">
                            <textarea id="process-group-comments" class="setting-input"></textarea>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-comments" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><%--Process group FlowFile concurrency--%>进程组的FlowFile并发</div>
                        <div class="editable setting-field">
                            <div id="process-group-flowfile-concurrency-combo"></div>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-flowfile-concurrency" class="unset"></span>
                        </div>
                    </div>
                    <div class="setting">
                        <div class="setting-name"><%--Process group outbound policy--%>进程组出站策略</div>
                        <div class="editable setting-field">
                            <div id="process-group-outbound-policy-combo"></div>
                        </div>
                        <div class="read-only setting-field">
                            <span id="read-only-process-group-outbound-policy" class="unset"></span>
                        </div>
                    </div>
                    <div class="editable settings-buttons">
                        <div id="process-group-configuration-save" class="button"><%--Apply--%>应用</div>
                        <div class="clear"></div>
                    </div>
                </div>
            </div>
            <div id="process-group-controller-services-tab-content" class="configuration-tab">
                <div id="process-group-controller-services-table" class="settings-table"></div>
            </div>
        </div>
    </div>
    <div id="process-group-refresh-container">
        <button id="process-group-configuration-refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
        <div class="last-refreshed-container">
            <%--Last updated--%>最后更新:&nbsp;<span id="process-group-configuration-last-refreshed" class="last-refreshed"></span>
        </div>
        <div id="process-group-configuration-loading-container" class="loading-container"></div>
        <%--<div id="flow-cs-availability" class="hidden">Listed services are available to all descendant Processors and services of this Process Group.</div>--%>
        <div id="flow-cs-availability" class="hidden">列出的服务可用于该进程组的所有后代处理器和服务。</div>
        <div class="clear"></div>
    </div>
</div>
