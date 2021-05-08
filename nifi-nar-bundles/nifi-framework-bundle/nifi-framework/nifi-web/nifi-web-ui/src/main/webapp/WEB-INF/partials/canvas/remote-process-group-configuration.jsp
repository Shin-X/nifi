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
<div id="remote-process-group-configuration" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name">名称</div>
            <%--<div class="setting-name">Name</div>--%>
            <div class="setting-field">
                <span id="remote-process-group-name"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">编号</div>
            <%--<div class="setting-name">Id</div>--%>
            <div class="setting-field">
                <span id="remote-process-group-id"></span>
            </div>
        </div>
        <div class="setting">
            <div class="setting-name">链接</div>
            <%--<div class="setting-name">URLs</div>--%>
            <div class="setting-field">
                <input type="text" id="remote-process-group-urls" class="setting-input"/>
            </div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--Transport Protocol--%>传输协议
                    <div class="fa fa-question-circle" alt="Info" title="指定用于此远程进程组通信的传输协议。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify the transport protocol to use for this Remote Process Group communication."></div>--%>
                </div>
                <div class="setting-field">
                    <div id="remote-process-group-transport-protocol-combo"></div>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--Local Network Interface--%>本地网络接口
                    <div class="fa fa-question-circle" alt="Info" title="发送/接收数据的本地网络接口。如果未指定，则使用任何本地地址。如果群集，所有节点都必须有一个具有此标识符的接口。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="The local network interface to send/receive data. If not specified, any local address is used. If clustered, all nodes must have an interface with this identifier."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-local-network-interface"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--HTTP Proxy server hostname--%>HTTP代理服务器主机名
                    <div class="fa fa-question-circle" alt="Info" title="指定要使用的代理服务器主机名。如果未指定，HTTP流量将直接发送到目标NiFi实例。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify the proxy server's hostname to use. If not specified, HTTP traffics are sent directly to the target NiFi instance."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-proxy-host"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--HTTP Proxy server port--%>HTTP代理服务器端口
                    <div class="fa fa-question-circle" alt="Info" title="指定代理服务器的端口号，可选。如果未指定，将使用默认端口80。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify the proxy server's port number, optional. If not specified, default port 80 will be used."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-proxy-port"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--HTTP Proxy user--%>HTTP代理用户
                    <div class="fa fa-question-circle" alt="Info" title="指定连接到代理服务器的用户名(可选)。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify an user name to connect to the proxy server, optional."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-proxy-user"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--HTTP Proxy password--%>HTTP代理密码
                    <div class="fa fa-question-circle" alt="Info" title="指定连接代理服务器的用户密码(可选)。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify an user password to connect to the proxy server, optional."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="password" class="small-setting-input" id="remote-process-group-proxy-password"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--Communications timeout--%>通讯超时
                    <div class="fa fa-question-circle" alt="Info" title="当与此远程进程组的通信花费的时间超过这个时间时，它将超时。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="When communication with this remote process group takes longer than this amount of time, it will timeout."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-timeout"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--Yield duration--%>收益率持续时间
                    <div class="fa fa-question-circle" alt="Info" title="当与此远程进程组的通信失败时，将不会再次对其进行调度，直到此时间结束。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="When communication with this remote process group fails, it will not be scheduled again until this amount of time elapses."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="remote-process-group-yield-duration"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>