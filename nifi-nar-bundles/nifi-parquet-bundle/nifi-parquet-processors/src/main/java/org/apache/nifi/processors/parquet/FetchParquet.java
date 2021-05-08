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
package org.apache.nifi.processors.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.AbstractFetchHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordReader;
import org.apache.nifi.parquet.hadoop.AvroParquetHDFSRecordReader;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import java.io.IOException;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"parquet", "hadoop", "HDFS", "get", "ingest", "fetch", "source", "record"})
@CapabilityDescription("读取给定的拼花文件，并使用选定的记录写入器将记录写入流文件的内容。原有的拼花文件将保持不变，流程文件的内容将替换为所选类型的记录。这个处理器可以与ListHDFS或ListFile一起使用，以获取要获取的文件列表。")
@WritesAttributes({
        @WritesAttribute(attribute="fetch.failure.reason", description="When a FlowFile is routed to 'failure', this attribute is added " +
                "indicating why the file could not be fetched from the given filesystem."),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the resulting flow file")
})
@SeeAlso({PutParquet.class})
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.READ_DISTRIBUTED_FILESYSTEM,
        explanation = "Provides operator the ability to retrieve any file that NiFi has access to in HDFS or the local filesystem.")
})
public class FetchParquet extends AbstractFetchHDFSRecord {

    @Override
    public HDFSRecordReader createHDFSRecordReader(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path)
            throws IOException {
        final ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(path).withConf(conf);
        return new AvroParquetHDFSRecordReader(readerBuilder.build());
    }

}
