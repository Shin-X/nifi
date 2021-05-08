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
<div id="history-filter-dialog" class="hidden medium-dialog">
    <div class="dialog-content">
        <div class="setting">
            <div class="setting-name"><%--Filter--%>过滤器</div>
            <div class="setting-field">
                <div id="history-filter-controls">
                    <input type="text" id="history-filter" class="history-large-input"/>
                    <div id="history-filter-type"></div>
                    <div class="clear"></div>
                </div>
            </div>
        </div>
        <div class="setting">
            <div class="start-date-setting">
                <div class="setting-name">
                    <%--Start date--%>开始日期
                    <div class="fa fa-question-circle" alt="Info" title="开始日期格式为‘mm/dd/yyyy’。还必须指定开始时间。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="The start date in the format 'mm/dd/yyyy'. Must also specify start time."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-start-date" class="history-small-input"/>
                </div>
            </div>
            <div class="start-time-setting">
                <div class="setting-name">
                    <%--Start time--%>开始时间 (<span class="timezone"></span>)
                    <div class="fa fa-question-circle" alt="Info" title="起始时间格式为“hh:mm:ss”。还必须指定开始日期。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="The start time in the format 'hh:mm:ss'. Must also specify start date."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-start-time" class="history-small-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
        <div class="setting">
            <div class="end-date-setting">
                <div class="setting-name">
                    <%--End date--%>结束日期
                    <div class="fa fa-question-circle" alt="Info" title="结束日期格式为'mm/dd/yyyy'。还必须指定结束时间。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="The end date in the format 'mm/dd/yyyy'. Must also specify end time."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-end-date" class="history-small-input"/>
                </div>
            </div>
            <div class="end-time-setting">
                <div class="setting-name">
                    <%--End time--%>结束时间 (<span class="timezone"></span>)
                    <div class="fa fa-question-circle" alt="Info" title="格式为“hh:mm:ss”的结束时间。还必须指定结束日期。"></div>
                    <%--<div class="fa fa-question-circle" alt="Info" title="The end time in the format 'hh:mm:ss'. Must also specify end date."></div>--%>
                </div>
                <div class="setting-field">
                    <input type="text" id="history-filter-end-time" class="history-small-input"/>
                </div>
            </div>
            <div class="clear"></div>
        </div>
    </div>
</div>
