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
<%@ page contentType="text/html" pageEncoding="UTF-8" session="false" %>
<div id="system-diagnostics-dialog" class="hidden large-dialog">
    <div class="dialog-content">
        <div id="system-diagnostics-tabs" class="tab-container"></div>
        <div id="system-diagnostics-tabs-content">
            <div id="jvm-tab-content" class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <input type="hidden" id=""/>
                        <div class="setting-header"><%--Heap--%>堆 <span id="utilization-heap"></span></div>
                        <div class="setting-field">
                            <table id="heap-table">
                                <tbody>
                                <tr>
                                    <td class="memory-header setting-name"><%--Max--%>最大:</td>
                                </tr>
                                <tr>
                                    <td><span id="max-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><%--Total--%>总计:</td>
                                </tr>
                                <tr>
                                    <td><span id="total-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><%--Used--%>使用:</td>
                                </tr>
                                <tr>
                                    <td><span id="used-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><%--Free--%>空闲:</td>
                                </tr>
                                <tr>
                                    <td><span id="free-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                </tbody>
                            </table>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="spacer"></div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-header"><%--Non-heap--%>非堆 <span id="utilization-non-heap"></span></div>
                        <div class="setting-field">
                            <table id="non-heap-table">
                                <tbody>
                                <tr>
                                    <td class="memory-header setting-name"><%--Max--%>最大:</td>
                                </tr>
                                <tr>
                                    <td><span id="max-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><%--Total--%>总计:</td>
                                </tr>
                                <tr>
                                    <td><span id="total-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><%--Used--%>使用:</td>
                                </tr>
                                <tr>
                                    <td><span id="used-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                <tr>
                                    <td class="setting-name"><%--Free--%>空闲:</td>
                                </tr>
                                <tr>
                                    <td><span id="free-non-heap"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                                </tbody>
                            </table>
                        </div>
                        <div class="clear"></div>
                    </div>
                </div>
                <div class="clear"></div>
                <div class="setting">
                    <div class="setting-header"><%--Garbage Collection--%>垃圾回收</div>
                    <div id="garbage-collection-container" class="setting-field">
                        <table id="garbage-collection-table">
                            <tbody></tbody>
                        </table>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-header"><%--Runtime--%>运行时间</div>
                    <div id="jvm-runtime-container" class="setting-field">
                        <table id="jvm-runtime-table">
                            <tbody>
                                <tr>
                                    <td class="setting-name"><%--Uptime--%>正常运行时间:</td>
                                </tr>
                                <tr>
                                    <td><span id="uptime"></span></td>
                                </tr>
                                <tr>
                                    <td></td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
            <div id="system-tab-content"class="configuration-tab">
                <div class="settings-left">
                    <div class="setting">
                        <div class="setting-name"><%--Available Cores--%>可用内核:</div>
                        <div class="setting-field">
                            <div id="available-processors"></div>
                        </div>
                    </div>
                </div>
                <div class="spacer"></div>
                <div class="settings-right">
                    <div class="setting">
                        <div class="setting-name">
                            <%--Core Load Average--%>内核负载均衡:
                            <div class="fa fa-question-circle" alt="Info" title="最后一分钟的平均核心负载。不能在所有平台上使用。"></div>
                            <%--<div class="fa fa-question-circle" alt="Info" title="Core load average for the last minute. Not available on all platforms."></div>--%>
                        </div>
                        <div class="setting-field">
                            <div id="processor-load-average"></div>
                        </div>
                    </div>
                </div>
                <div class="clear"></div>
                <div class="setting">
                    <div class="setting-header"><%--FlowFile Repository Storage--%>FlowFile库存储</div>
                    <div class="setting-field">
                        <div id="flow-file-repository-storage-usage-container"></div>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-header"><%--Content Repository Storage--%>Content存储库</div>
                    <div class="setting-field">
                        <div id="content-repository-storage-usage-container"></div>
                    </div>
                </div>
                <div class="setting">
                    <div class="setting-header"><%--Provenance Repository Storage--%>Provenance库存储</div>
                    <div class="setting-field">
                        <div id="provenance-repository-storage-usage-container"></div>
                    </div>
                </div>
            </div>
            <div id="version-tab-content" class="configuration-tab">
                <div class="setting">
                    <div class="setting-header">NiFi</div>
                    <dl class="setting-attributes-list">
                        <dt><%--NiFi Version--%>NiFi版本</dt><dd><span id="version-nifi"></span></dd>
                        <dt><%--Tag--%>标签</dt><dd><span id="version-build-tag"></span></dd>
                        <dt><%--Build Date/Time--%>构建日期/时间</dt><dd><span id="version-build-timestamp"></span></dd>
                        <dt><%--Branch--%>分支</dt><dd><span id="version-build-branch"></span></dd>
                        <dt><%--Revision--%>修订本</dt><dd><span id="version-build-revision"></span></dd>
                    </dl>
                </div>
                <div class="setting">
                    <div class="setting-header">Java</div>
                    <dl class="setting-attributes-list">
                        <dt><%--Version--%>版本</dt><dd><span id="version-java-version"></span></dd>
                        <dt><%--Vendor--%>供应商</dt><dd><span id="version-java-vendor"></span></dd>
                    </dl>
                </div>
                <div class="setting">
                    <div class="setting-header"><%--Operating System--%>操作系统</div>
                    <dl class="setting-attributes-list">
                        <dt><%--Name--%>名称</dt><dd><span id="version-os-name"></span></dd>
                        <dt><%--Version--%>版本</dt><dd><span id="version-os-version"></span></dd>
                        <dt><%--Architecture--%>架构</dt><dd><span id="version-os-arch"></span></dd>
                    </dl>
                </div>
            </div>
        </div>
        <div id="system-diagnostics-refresh-container">
            <button id="system-diagnostics-refresh-button" class="refresh-button pointer fa fa-refresh" title="Refresh"></button>
            <div id="system-diagnostics-last-refreshed-container" class="last-refreshed-container">
                <span id="system-diagnostics-last-refreshed" class="value-color"></span>
            </div>
            <div id="system-diagnostics-loading-container" class="loading-container"></div>
        </div>
    </div>
</div>
