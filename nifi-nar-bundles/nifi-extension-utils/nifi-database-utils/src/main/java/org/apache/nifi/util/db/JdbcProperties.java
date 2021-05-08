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
package org.apache.nifi.util.db;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public class JdbcProperties {

    public static final PropertyDescriptor NORMALIZE_NAMES_FOR_AVRO = new PropertyDescriptor.Builder()
            .name("dbf-normalize")
            .displayName("表名或列名正常化")
            .description("是否将列名中不兼容avro的字符修改为兼容avro的字符。例如，冒号和句点将被更改为下划线，以构建有效的Avro记录。")
//            .displayName("Normalize Table/Column Names")
//            .description("Whether to change non-Avro-compatible characters in column names to Avro-compatible characters. For example, colons and periods "
//                    + "will be changed to underscores in order to build a valid Avro record.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor USE_AVRO_LOGICAL_TYPES = new PropertyDescriptor.Builder()
            .name("dbf-user-logical-types")
            .displayName("使用Avro逻辑类型")
            .description("是否为十进制/数字、日期、时间和时间戳列使用Avro逻辑类型。如果禁用，则以字符串形式写入。如果启用,逻辑类型和书面作为其基本类型,具体来说,十进制/数字为逻辑“小数”:写成字节与额外的元数据精度和规模,日期为逻辑“date-millis”:写成int表示天Unix纪元(1970-01-01),时间为逻辑“time-millis”:写成int表示毫秒Unix纪元以来,和时间戳为逻辑“timestamp-millis”:写只要表示Unix纪元以来的毫秒。如果已写Avro记录的读取器也知道这些逻辑类型，那么可以根据读取器的实现使用更多上下文反序列化这些值。")
//            .displayName("Use Avro Logical Types")
//            .description("Whether to use Avro Logical Types for DECIMAL/NUMBER, DATE, TIME and TIMESTAMP columns. "
//                    + "If disabled, written as string. "
//                    + "If enabled, Logical types are used and written as its underlying type, specifically, "
//                    + "DECIMAL/NUMBER as logical 'decimal': written as bytes with additional precision and scale meta data, "
//                    + "DATE as logical 'date-millis': written as int denoting days since Unix epoch (1970-01-01), "
//                    + "TIME as logical 'time-millis': written as int denoting milliseconds since Unix epoch, "
//                    + "and TIMESTAMP as logical 'timestamp-millis': written as long denoting milliseconds since Unix epoch. "
//                    + "If a reader of written Avro records also knows these logical types, then these values can be deserialized with more context depending on reader implementation.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_PRECISION = new PropertyDescriptor.Builder()
            .name("dbf-default-precision")
            .displayName("默认的小数精度")
            .description("当十进制/数字值被写入' DECIMAL ' Avro逻辑类型时，需要指定表示可用位数的特定'precision'。通常，精度由列数据类型定义或数据库引擎默认值定义。但是，未定义的精度(0)可以从某些数据库引擎返回。'Default Decimal Precision'在写入那些未定义精度的数字时使用。")
//            .displayName("Default Decimal Precision")
//            .description("When a DECIMAL/NUMBER value is written as a 'decimal' Avro logical type,"
//                    + " a specific 'precision' denoting number of available digits is required."
//                    + " Generally, precision is defined by column data type definition or database engines default."
//                    + " However undefined precision (0) can be returned from some database engines."
//                    + " 'Default Decimal Precision' is used when writing those undefined precision numbers.")
            .defaultValue(String.valueOf(JdbcCommon.DEFAULT_PRECISION_VALUE))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor DEFAULT_SCALE = new PropertyDescriptor.Builder()
            .name("dbf-default-scale")
            .displayName("默认十进位制")
            .description("当一个十进制/数字值被写入“DECIMAL”Avro逻辑类型时，需要一个特定的“刻度”，表示可用的十进制数字的数量。通常，伸缩是由列数据类型定义或数据库引擎默认定义的。然而，当返回未定义的精度(0)时，对于某些数据库引擎，规模也可能是不确定的。当写这些未定义的数字时使用'Default Decimal Scale'。如果一个值的小数数多于指定的比例，那么该值将被舍入，例如，尺度0的1.53变成2，尺度1的1.5变成1.5。")
//            .displayName("Default Decimal Scale")
//            .description("When a DECIMAL/NUMBER value is written as a 'decimal' Avro logical type,"
//                    + " a specific 'scale' denoting number of available decimal digits is required."
//                    + " Generally, scale is defined by column data type definition or database engines default."
//                    + " However when undefined precision (0) is returned, scale can also be uncertain with some database engines."
//                    + " 'Default Decimal Scale' is used when writing those undefined numbers."
//                    + " If a value has more decimals than specified scale, then the value will be rounded-up,"
//                    + " e.g. 1.53 becomes 2 with scale 0, and 1.5 with scale 1.")
            .defaultValue(String.valueOf(JdbcCommon.DEFAULT_SCALE_VALUE))
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    // Registry-only versions of Default Precision and Default Scale properties
    public static final PropertyDescriptor VARIABLE_REGISTRY_ONLY_DEFAULT_PRECISION =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(DEFAULT_PRECISION)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();

    public static final PropertyDescriptor VARIABLE_REGISTRY_ONLY_DEFAULT_SCALE =
            new PropertyDescriptor.Builder().fromPropertyDescriptor(DEFAULT_SCALE)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();
}
