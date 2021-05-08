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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.serialization.record.Record;

import java.util.ArrayList;
import java.util.List;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({"convert", "record", "generic", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
    @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile"),
    @WritesAttribute(attribute = "record.error.message", description = "This attribute provides on failure the error message encountered by the Reader or Writer.")
})
@CapabilityDescription("使用配置的记录读取器和记录写入控制器服务将记录从一种数据格式转换为另一种数据格式。" +
        "读取器和写入器必须配置为\"matching\"模式。这里的意思是模式必须具有相同的字段名。如果字段值可以从一种类型强制转换为另一种类型，" +
        "则字段的类型不必相同。例如，如果输入模式有一个名为\"balance\"的类型为double的字段，那么输出模式可以有一个名为\"balance\"的类型为string、" +
        "double或float的字段。如果输入中的任何字段没有出现在输出中，那么该字段将被排除在输出中。如果在输出模式中指定了任何字段，但在输入数据/模式中不存在，" +
        "那么该字段将不会出现在输出中，或者将有一个空值，这取决于写入器。")
//@CapabilityDescription("Converts records from one data format to another using configured Record Reader and Record Write Controller Services. "
//    + "The Reader and Writer must be configured with \"matching\" schemas. By this, we mean the schemas must have the same field names. The types of the fields "
//    + "do not have to be the same if a field value can be coerced from one type to another. For instance, if the input schema has a field named \"balance\" of type double, "
//    + "the output schema can have a field named \"balance\" with a type of string, double, or float. If any field is present in the input that is not present in the output, "
//    + "the field will be left out of the output. If any field is specified in the output schema but is not present in the input data/schema, then the field will not be "
//    + "present in the output or will have a null value, depending on the writer.")
public class ConvertRecord extends AbstractRecordProcessor {

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        return properties;
    }

    @Override
    protected Record process(final Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        return record;
    }

}
