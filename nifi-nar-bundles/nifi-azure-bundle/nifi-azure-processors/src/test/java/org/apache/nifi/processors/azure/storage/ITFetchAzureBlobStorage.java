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
package org.apache.nifi.processors.azure.storage;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.azure.AbstractAzureBlobProcessor;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ITFetchAzureBlobStorage extends AbstractAzureBlobStorageIT {

    @Override
    protected Class<? extends Processor> getProcessorClass() {
        return FetchAzureBlobStorage.class;
    }

    @Before
    public void setUp() throws Exception {
        runner.setProperty(FetchAzureBlobStorage.BLOB, TEST_BLOB_NAME);

        uploadTestBlob();
    }

    @Test
    public void testFetchBlob() throws Exception {
        runner.assertValid();
        runner.enqueue(new byte[0]);
        runner.run();

        assertResult();
    }

    @Test
    public void testFetchBlobUsingCredentialService() throws Exception {
        configureCredentialsService();

        runner.assertValid();
        runner.enqueue(new byte[0]);
        runner.run();

        assertResult();
    }

    private void assertResult() throws Exception {
        runner.assertAllFlowFilesTransferred(AbstractAzureBlobProcessor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(FetchAzureBlobStorage.REL_SUCCESS);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertContentEquals("0123456789".getBytes());
            flowFile.assertAttributeEquals("azure.length", "10");
        }
    }
}
