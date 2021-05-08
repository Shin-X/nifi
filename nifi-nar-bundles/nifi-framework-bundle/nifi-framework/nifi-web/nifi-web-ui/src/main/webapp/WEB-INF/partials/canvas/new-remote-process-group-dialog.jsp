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
<div id="new-remote-process-group-dialog" class="hidden large-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><%--URLs--%>链接
                <%--<div class="fa fa-question-circle" alt="Info" title="Specify the remote target NiFi URLs. Multiple URLs can be specified in comma-separated format. Different protocols cannot be mixed. If remote NiFi is a cluster, two or more node URLs are recommended for better connection establishment availability."></div>--%>
                <div class="fa fa-question-circle" alt="Info" title="指定远程目标NiFi的url。可以用逗号分隔的格式指定多个url。不同的协议不能混合。如果远端NiFi为集群，建议使用两个或两个以上的节点url，以提高连接建立的可靠性。"></div>
             </div>
            <div class="setting-field">
                <input id="new-remote-process-group-uris" type="text" placeholder="https://remotehost:8443/nifi"/>
            </div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--Transport Protocol--%>传输协议
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify the transport protocol to use for this Remote Process Group communication."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="指定用于此远程进程组通信的传输协议。"></div>
                </div>
                <div class="setting-field">
                    <div id="new-remote-process-group-transport-protocol-combo"></div>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--Local Network Interface--%>本地网络接口
                    <%--<div class="fa fa-question-circle" alt="Info" title="The local network interface to send/receive data. If not specified, any local address is used. If clustered, all nodes must have an interface with this identifier."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="发送/接收数据的本地网络接口。如果未指定，则使用任何本地地址。如果是群集，所有节点都必须有一个具有此标识符的接口。"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-local-network-interface"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--HTTP Proxy server hostname--%>HTTP代理服务器主机名
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify the proxy server's hostname to use. If not specified, HTTP traffics are sent directly to the target NiFi instance."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="指定要使用的代理服务器主机名。如果未指定，HTTP流量将直接发送到目标NiFi实例。"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-host"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--HTTP Proxy server port--%>HTTP代理服务器端口
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify the proxy server's port number, optional. If not specified, default port 80 will be used."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="指定代理服务器的端口号，可选。如果未指定，将使用默认端口80。"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-port"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--HTTP Proxy user--%>HTTP代理用户
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify an user name to connect to the proxy server, optional."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="指定连接到代理服务器的用户名(可选)。"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-proxy-user"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--HTTP Proxy password--%>HTTP代理密码
                    <%--<div class="fa fa-question-circle" alt="Info" title="Specify an user password to connect to the proxy server, optional."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="指定连接代理服务器的用户密码(可选)。"></div>
                </div>
                <div class="setting-field">
                    <input type="password" class="small-setting-input" id="new-remote-process-group-proxy-password"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="remote-process-group-setting-left">
                <div class="setting-name">
                    <%--Communications timeout--%>通讯超时
                    <%--<div class="fa fa-question-circle" alt="Info" title="When communication with this remote process group takes longer than this amount of time, it will timeout."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="当与此远程进程组的通信花费的时间超过这个时间时，它将超时。"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-timeout"/>
                </div>
            </div>
            <div class="remote-process-group-setting-right">
                <div class="setting-name">
                    <%--Yield duration--%>收益率持续时间
                    <%--<div class="fa fa-question-circle" alt="Info" title="When communication with this remote process group fails, it will not be scheduled again until this amount of time elapses."></div>--%>
                    <div class="fa fa-question-circle" alt="Info" title="当与此远程进程组的通信失败时，将不会再次对其进行调度，直到此时间结束。"></div>
                </div>
                <div class="setting-field">
                    <input type="text" class="small-setting-input" id="new-remote-process-group-yield-duration"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>