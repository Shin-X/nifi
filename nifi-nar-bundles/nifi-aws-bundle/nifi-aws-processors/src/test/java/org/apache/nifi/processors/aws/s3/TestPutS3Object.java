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
package org.apache.nifi.processors.aws.s3;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestPutS3Object {

    private TestRunner runner;
    private PutS3Object putS3Object;
    private AmazonS3Client mockS3Client;

    @Before
    public void setUp() {
        mockS3Client = Mockito.mock(AmazonS3Client.class);
        putS3Object = new PutS3Object() {
            @Override
            protected AmazonS3Client getClient() {
                return mockS3Client;
            }
        };
        runner = TestRunners.newTestRunner(putS3Object);

        // MockPropertyValue does not evaluate system properties, set it in a variable with the same name
        runner.setVariable("java.io.tmpdir", System.getProperty("java.io.tmpdir"));
    }

    @Test
    public void testPutSinglePart() {
        runner.setProperty("x-custom-prop", "hello");
        prepareTest();

        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
        PutObjectRequest request = captureRequest.getValue();
        assertEquals("test-bucket", request.getBucketName());

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutS3Object.REL_SUCCESS);
        MockFlowFile ff0 = flowFiles.get(0);

        ff0.assertAttributeEquals(CoreAttributes.FILENAME.key(), "testfile.txt");
        ff0.assertAttributeEquals(PutS3Object.S3_ETAG_ATTR_KEY, "test-etag");
        ff0.assertAttributeEquals(PutS3Object.S3_VERSION_ATTR_KEY, "test-version");
    }

    @Test
    public void testPutSinglePartException() {
        prepareTest();

        Mockito.when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class))).thenThrow(new AmazonS3Exception("TestFail"));

        runner.run(1);

        runner.assertAllFlowFilesTransferred(PutS3Object.REL_FAILURE, 1);
    }

    @Test
    public void testSignerOverrideOptions() {
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final ClientConfiguration config = new ClientConfiguration();
        final PutS3Object processor = new PutS3Object();
        final TestRunner runner = TestRunners.newTestRunner(processor);

        final List<AllowableValue> allowableSignerValues = PutS3Object.SIGNER_OVERRIDE.getAllowableValues();
        final String defaultSignerValue = PutS3Object.SIGNER_OVERRIDE.getDefaultValue();

        for (AllowableValue allowableSignerValue : allowableSignerValues) {
            String signerType = allowableSignerValue.getValue();
            if (!signerType.equals(defaultSignerValue)) {
                runner.setProperty(PutS3Object.SIGNER_OVERRIDE, signerType);
                ProcessContext context = runner.getProcessContext();
                try {
                    processor.createClient(context, credentialsProvider, config);
                } catch (IllegalArgumentException argEx) {
                    Assert.fail(argEx.getMessage());
                }
            }
        }
    }

    @Test
    public void testObjectTags() {
        runner.setProperty(PutS3Object.OBJECT_TAGS_PREFIX, "tagS3");
        runner.setProperty(PutS3Object.REMOVE_TAG_PREFIX, "false");
        prepareTest();

        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
        PutObjectRequest request = captureRequest.getValue();

        List<Tag> tagSet = request.getTagging().getTagSet();

        assertEquals(1, tagSet.size());
        assertEquals("tagS3PII", tagSet.get(0).getKey());
        assertEquals("true", tagSet.get(0).getValue());
    }

    @Test
    public void testStorageClasses() {
        for (StorageClass storageClass : StorageClass.values()) {
            runner.setProperty(PutS3Object.STORAGE_CLASS, storageClass.name());
            prepareTest();

            runner.run(1);

            ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
            Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
            PutObjectRequest request = captureRequest.getValue();

            assertEquals(storageClass.toString(), request.getStorageClass());

            Mockito.reset(mockS3Client);
        }
    }

    @Test
    public void testFilenameWithNationalCharacters() throws UnsupportedEncodingException {
        prepareTest("Iñtërnâtiônàližætiøn.txt");

        runner.run(1);

        ArgumentCaptor<PutObjectRequest> captureRequest = ArgumentCaptor.forClass(PutObjectRequest.class);
        Mockito.verify(mockS3Client, Mockito.times(1)).putObject(captureRequest.capture());
        PutObjectRequest request = captureRequest.getValue();

        ObjectMetadata objectMetadata = request.getMetadata();
        assertEquals(URLEncoder.encode("Iñtërnâtiônàližætiøn.txt", "UTF-8"), objectMetadata.getContentDisposition());
    }

    private void prepareTest() {
        prepareTest("testfile.txt");
    }

    private void prepareTest(String filename) {
        runner.setProperty(PutS3Object.REGION, "ap-northeast-1");
        runner.setProperty(PutS3Object.BUCKET, "test-bucket");
        runner.assertValid();

        Map<String, String> ffAttributes = new HashMap<>();
        ffAttributes.put("filename", filename);
        ffAttributes.put("tagS3PII", "true");
        runner.enqueue("Test Content", ffAttributes);

        PutObjectResult putObjectResult = new PutObjectResult();
        putObjectResult.setExpirationTime(new Date());
        putObjectResult.setMetadata(new ObjectMetadata());
        putObjectResult.setVersionId("test-version");
        putObjectResult.setETag("test-etag");

        Mockito.when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class))).thenReturn(putObjectResult);

        MultipartUploadListing uploadListing = new MultipartUploadListing();
        Mockito.when(mockS3Client.listMultipartUploads(Mockito.any(ListMultipartUploadsRequest.class))).thenReturn(uploadListing);
    }

    @Test
    public void testPersistenceFileLocationWithDefaultTempDir() {
        String dir = System.getProperty("java.io.tmpdir");

        executePersistenceFileLocationTest(StringUtils.appendIfMissing(dir, File.separator) + putS3Object.getIdentifier());
    }

    @Test
    public void testPersistenceFileLocationWithUserDefinedDirWithEndingSeparator() {
        String dir = StringUtils.appendIfMissing(new File("target").getAbsolutePath(), File.separator);
        runner.setProperty(PutS3Object.MULTIPART_TEMP_DIR, dir);

        executePersistenceFileLocationTest(dir + putS3Object.getIdentifier());
    }

    @Test
    public void testPersistenceFileLocationWithUserDefinedDirWithoutEndingSeparator() {
        String dir = StringUtils.removeEnd(new File("target").getAbsolutePath(), File.separator);
        runner.setProperty(PutS3Object.MULTIPART_TEMP_DIR, dir);

        executePersistenceFileLocationTest(dir + File.separator + putS3Object.getIdentifier());
    }

    private void executePersistenceFileLocationTest(String expectedPath) {
        prepareTest();

        runner.run(1);
        File file = putS3Object.getPersistenceFile();

        assertEquals(expectedPath, file.getAbsolutePath());
    }

    @Test
    public void testGetPropertyDescriptors() {
        PutS3Object processor = new PutS3Object();
        List<PropertyDescriptor> pd = processor.getSupportedPropertyDescriptors();
        assertEquals("size should be eq", 39, pd.size());
        assertTrue(pd.contains(PutS3Object.ACCESS_KEY));
        assertTrue(pd.contains(PutS3Object.AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(pd.contains(PutS3Object.BUCKET));
        assertTrue(pd.contains(PutS3Object.CANNED_ACL));
        assertTrue(pd.contains(PutS3Object.CREDENTIALS_FILE));
        assertTrue(pd.contains(PutS3Object.ENDPOINT_OVERRIDE));
        assertTrue(pd.contains(PutS3Object.FULL_CONTROL_USER_LIST));
        assertTrue(pd.contains(PutS3Object.KEY));
        assertTrue(pd.contains(PutS3Object.OWNER));
        assertTrue(pd.contains(PutS3Object.READ_ACL_LIST));
        assertTrue(pd.contains(PutS3Object.READ_USER_LIST));
        assertTrue(pd.contains(PutS3Object.REGION));
        assertTrue(pd.contains(PutS3Object.SECRET_KEY));
        assertTrue(pd.contains(PutS3Object.SIGNER_OVERRIDE));
        assertTrue(pd.contains(PutS3Object.SSL_CONTEXT_SERVICE));
        assertTrue(pd.contains(PutS3Object.TIMEOUT));
        assertTrue(pd.contains(PutS3Object.EXPIRATION_RULE_ID));
        assertTrue(pd.contains(PutS3Object.STORAGE_CLASS));
        assertTrue(pd.contains(PutS3Object.WRITE_ACL_LIST));
        assertTrue(pd.contains(PutS3Object.WRITE_USER_LIST));
        assertTrue(pd.contains(PutS3Object.SERVER_SIDE_ENCRYPTION));
        assertTrue(pd.contains(PutS3Object.ENCRYPTION_SERVICE));
        assertTrue(pd.contains(PutS3Object.USE_CHUNKED_ENCODING));
        assertTrue(pd.contains(PutS3Object.USE_PATH_STYLE_ACCESS));
        assertTrue(pd.contains(PutS3Object.PROXY_CONFIGURATION_SERVICE));
        assertTrue(pd.contains(PutS3Object.PROXY_HOST));
        assertTrue(pd.contains(PutS3Object.PROXY_HOST_PORT));
        assertTrue(pd.contains(PutS3Object.PROXY_USERNAME));
        assertTrue(pd.contains(PutS3Object.PROXY_PASSWORD));
        assertTrue(pd.contains(PutS3Object.OBJECT_TAGS_PREFIX));
        assertTrue(pd.contains(PutS3Object.REMOVE_TAG_PREFIX));
        assertTrue(pd.contains(PutS3Object.CONTENT_TYPE));
        assertTrue(pd.contains(PutS3Object.CONTENT_DISPOSITION));
        assertTrue(pd.contains(PutS3Object.CACHE_CONTROL));
        assertTrue(pd.contains(PutS3Object.MULTIPART_THRESHOLD));
        assertTrue(pd.contains(PutS3Object.MULTIPART_PART_SIZE));
        assertTrue(pd.contains(PutS3Object.MULTIPART_S3_AGEOFF_INTERVAL));
        assertTrue(pd.contains(PutS3Object.MULTIPART_S3_MAX_AGE));
        assertTrue(pd.contains(PutS3Object.MULTIPART_TEMP_DIR));
    }
}
