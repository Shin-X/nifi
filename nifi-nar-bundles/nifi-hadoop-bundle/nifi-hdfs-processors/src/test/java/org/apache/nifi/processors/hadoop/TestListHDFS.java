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
package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.nifi.processors.hadoop.ListHDFS.FILTER_DIRECTORIES_AND_FILES_VALUE;
import static org.apache.nifi.processors.hadoop.ListHDFS.FILTER_FILES_ONLY_VALUE;
import static org.apache.nifi.processors.hadoop.ListHDFS.FILTER_FULL_PATH_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestListHDFS {

    private TestRunner runner;
    private ListHDFSWithMockedFileSystem proc;
    private NiFiProperties mockNiFiProperties;
    private KerberosProperties kerberosProperties;
    private MockComponentLog mockLogger;

    @Before
    public void setup() throws InitializationException {
        mockNiFiProperties = mock(NiFiProperties.class);
        when(mockNiFiProperties.getKerberosConfigurationFile()).thenReturn(null);
        kerberosProperties = new KerberosProperties(null);

        proc = new ListHDFSWithMockedFileSystem(kerberosProperties);
        mockLogger = spy(new MockComponentLog(UUID.randomUUID().toString(), proc));
        runner = TestRunners.newTestRunner(proc, mockLogger);

        runner.setProperty(ListHDFS.HADOOP_CONFIGURATION_RESOURCES, "src/test/resources/core-site.xml");
        runner.setProperty(ListHDFS.DIRECTORY, "/test");
    }

    @Test
    public void testListingWithValidELFunction() throws InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.setProperty(ListHDFS.DIRECTORY, "${literal('/test'):substring(0,5)}");

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("path", "/test");
        mff.assertAttributeEquals("filename", "testFile.txt");
    }

    @Test
    public void testListingWithFilter() throws InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.setProperty(ListHDFS.DIRECTORY, "${literal('/test'):substring(0,5)}");
        runner.setProperty(ListHDFS.FILE_FILTER, "[^test].*");

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
    }

    @Test
    public void testListingWithInvalidELFunction() throws InterruptedException {
        runner.setProperty(ListHDFS.DIRECTORY, "${literal('/test'):foo()}");
        runner.assertNotValid();
    }

    @Test
    public void testListingWithUnrecognizedELFunction() throws InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        runner.setProperty(ListHDFS.DIRECTORY, "data_${literal('testing'):substring(0,4)%7D");

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
    }

    @Test
    public void testListingHasCorrectAttributes() throws InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("path", "/test");
        mff.assertAttributeEquals("filename", "testFile.txt");
    }


    @Test
    public void testRecursiveWithDefaultFilterAndFilterMode() throws InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/.testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i=0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("testFile.txt")) {
                ff.assertAttributeEquals("path", "/test");
            } else if ( filename.equals("1.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir");
            } else {
                Assert.fail("filename was " + filename);
            }
        }
    }

    @Test
    public void testRecursiveWithCustomFilterDirectoriesAndFiles() throws InterruptedException, IOException {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, ".*txt.*");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_DIRECTORIES_AND_FILES_VALUE.getValue());

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/txtDir")));
        proc.fileSystem.addFileStatus(new Path("/test/txtDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/txtDir/3.out")));
        proc.fileSystem.addFileStatus(new Path("/test/txtDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/txtDir/3.txt")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("testFile.txt")) {
                ff.assertAttributeEquals("path", "/test");
            } else if (filename.equals("3.txt")) {
                ff.assertAttributeEquals("path", "/test/txtDir");
            } else {
                Assert.fail("filename was " + filename);
            }
        }
    }

    @Test
    public void testRecursiveWithCustomFilterFilesOnly() throws InterruptedException, IOException {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, "[^\\.].*\\.txt");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_FILES_ONLY_VALUE.getValue());

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/.partfile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/.txt")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir/anotherDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/anotherDir/2.txt")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("testFile.txt")) {
                ff.assertAttributeEquals("path", "/test");
            } else if (filename.equals("1.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir");
            } else if (filename.equals("2.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else {
                Assert.fail("filename was " + filename);
            }
        }
    }

    @Test
    public void testRecursiveWithCustomFilterFullPathWithoutSchemeAndAuthority() throws InterruptedException, IOException {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, "(/.*/)*anotherDir/1\\..*");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_FULL_PATH_VALUE.getValue());

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("1.out")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else if (filename.equals("1.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else {
                Assert.fail("filename was " + filename);
            }
        }
    }

    @Test
    public void testRecursiveWithCustomFilterFullPathWithSchemeAndAuthority() throws InterruptedException, IOException {
        // set custom regex filter and filter mode
        runner.setProperty(ListHDFS.FILE_FILTER, "hdfs://hdfscluster:8020(/.*/)*anotherDir/1\\..*");
        runner.setProperty(ListHDFS.FILE_FILTER_MODE, FILTER_FULL_PATH_VALUE.getValue());

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS);
        for (int i = 0; i < 2; i++) {
            final MockFlowFile ff = flowFiles.get(i);
            final String filename = ff.getAttribute("filename");

            if (filename.equals("1.out")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else if (filename.equals("1.txt")) {
                ff.assertAttributeEquals("path", "/test/testDir/anotherDir");
            } else {
                Assert.fail("filename was " + filename);
            }
        }
    }

    @Test
    public void testNotRecursive() throws InterruptedException {
        runner.setProperty(ListHDFS.RECURSE_SUBDIRS, "false");
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");
    }


    @Test
    public void testNoListUntilUpdateFromRemoteOnPrimaryNodeChange() throws IOException, InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 1999L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        final MockFlowFile mff1 = runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0);
        mff1.assertAttributeEquals("path", "/test");
        mff1.assertAttributeEquals("filename", "testFile.txt");

        runner.clearTransferState();

        // add new file to pull
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 2000L, 0L, create777(), "owner", "group", new Path("/test/testFile2.txt")));

        runner.getStateManager().setFailOnStateGet(Scope.CLUSTER, true);

        // Should fail to perform @OnScheduled methods.
        try {
            runner.run();
            Assert.fail("Processor ran successfully");
        } catch (final AssertionError e) {
        }

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);

        // Should fail to perform @OnScheduled methods.
        try {
            runner.run();
            Assert.fail("Processor ran successfully");
        } catch (final AssertionError e) {
        }

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);

        runner.getStateManager().setFailOnStateGet(Scope.CLUSTER, false);
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));

        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
        Map<String, String> newState = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertEquals("2000", newState.get(ListHDFS.LISTING_TIMESTAMP_KEY));
        assertEquals("1999", newState.get(ListHDFS.EMITTED_TIMESTAMP_KEY));

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        newState = runner.getStateManager().getState(Scope.CLUSTER).toMap();
        assertEquals("2000", newState.get(ListHDFS.LISTING_TIMESTAMP_KEY));
        assertEquals("2000", newState.get(ListHDFS.EMITTED_TIMESTAMP_KEY));

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
    }

    @Test
    public void testOnlyNewestEntriesHeldBack() throws InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, 8L, 0L, create777(), "owner", "group", new Path("/test/testFile2.txt")));

        // this is a directory, so it won't be counted toward the entries
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, true, 1, 1L, 8L, 0L, create777(), "owner", "group", new Path("/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 100L, 0L, create777(), "owner", "group", new Path("/test/testDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 100L, 0L, create777(), "owner", "group", new Path("/test/testDir/2.txt")));

        // The first iteration should pick up 2 files with the smaller timestamps.
        runner.run();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        // Next iteration should pick up the other 2 files, since nothing else was added.
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 4);

        proc.fileSystem.addFileStatus(new Path("/test/testDir"), new FileStatus(1L, false, 1, 1L, 110L, 0L, create777(), "owner", "group", new Path("/test/testDir/3.txt")));
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 4);

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 5);
    }

    @Test
    public void testMinAgeMaxAge() throws IOException, InterruptedException {
        long now = new Date().getTime();
        long oneHourAgo = now - 3600000;
        long twoHoursAgo = now - 2*3600000;
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now, now, create777(), "owner", "group", new Path("/test/willBeIgnored.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, now-5, now-5, create777(), "owner", "group", new Path("/test/testFile.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, oneHourAgo, oneHourAgo, create777(), "owner", "group", new Path("/test/testFile1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test"), new FileStatus(1L, false, 1, 1L, twoHoursAgo, twoHoursAgo, create777(), "owner", "group", new Path("/test/testFile2.txt")));

        // all files
        runner.run();
        runner.assertValid();
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // invalid min_age > max_age
        runner.setProperty(ListHDFS.MIN_AGE, "30 sec");
        runner.setProperty(ListHDFS.MAX_AGE, "1 sec");
        runner.assertNotValid();

        // only one file (one hour ago)
        runner.setProperty(ListHDFS.MIN_AGE, "30 sec");
        runner.setProperty(ListHDFS.MAX_AGE, "90 min");
        runner.assertValid();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run(); // will ignore the file for this cycle
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        // Next iteration should pick up the file, since nothing else was added.
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(ListHDFS.REL_SUCCESS).get(0).assertAttributeEquals("filename", "testFile1.txt");
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // two files (one hour ago and two hours ago)
        runner.setProperty(ListHDFS.MIN_AGE, "30 sec");
        runner.removeProperty(ListHDFS.MAX_AGE);
        runner.assertValid();

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 1);

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);
        runner.clearTransferState();
        runner.getStateManager().clear(Scope.CLUSTER);

        // two files (now and one hour ago)
        runner.setProperty(ListHDFS.MIN_AGE, "0 sec");
        runner.setProperty(ListHDFS.MAX_AGE, "90 min");
        runner.assertValid();

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);
    }

    @Test
    public void testListAfterDirectoryChange() throws InterruptedException {
        proc.fileSystem.addFileStatus(new Path("/test1"), new FileStatus(1L, false, 1, 1L, 100L,0L, create777(), "owner", "group", new Path("/test1/testFile-1_1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test2"), new FileStatus(1L, false, 1, 1L, 150L,0L, create777(), "owner", "group", new Path("/test2/testFile-2_1.txt")));
        proc.fileSystem.addFileStatus(new Path("/test1"), new FileStatus(1L, false, 1, 1L, 200L,0L, create777(), "owner", "group", new Path("/test1/testFile-1_2.txt")));

        runner.setProperty(ListHDFS.DIRECTORY, "/test1");

        runner.run(); // Initial run, latest file from /test1 will be ignored

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run(); // Latest file i.e. testFile-1_2.txt from /test1 should also be picked up now
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        runner.setProperty(ListHDFS.DIRECTORY, "/test2"); // Changing directory should reset the state

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run(); // Will ignore the files for this cycle
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 2);

        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run(); // Since state has been reset, testFile-2_1.txt from /test2 should be picked up
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 3);
    }

    @Test
    public void testListingEmptyDir() throws InterruptedException, IOException {
        runner.setProperty(ListHDFS.DIRECTORY, "/test/emptyDir");

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/emptyDir")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        // verify that no messages were logged at the error level
        verify(mockLogger, never()).error(anyString());
        final ArgumentCaptor<Throwable> throwableArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockLogger, atLeast(0)).error(anyString(), throwableArgumentCaptor.capture());
        // if error.(message, throwable) was called, ignore JobConf CNFEs since mapreduce libs are not included as dependencies
        assertTrue(throwableArgumentCaptor.getAllValues().stream().flatMap(Stream::of)
                // check that there are no throwables that are not of JobConf CNFE exceptions
                .noneMatch(throwable -> !(throwable instanceof ClassNotFoundException && throwable.getMessage().contains("JobConf"))));
        verify(mockLogger, never()).error(anyString(), any(Object[].class));
        verify(mockLogger, never()).error(anyString(), any(Object[].class), any(Throwable.class));

        // assert that no files were listed
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
    }

    @Test
    public void testListingNonExistingDir() throws InterruptedException, IOException {
        String nonExistingPath = "/test/nonExistingDir";
        runner.setProperty(ListHDFS.DIRECTORY, nonExistingPath);

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testFile.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/emptyDir")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/1.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/1.txt")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.out")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/anotherDir/2.txt")));

        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir"),
                new FileStatus(1L, true, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir")));
        proc.fileSystem.addFileStatus(new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir"),
                new FileStatus(1L, false, 1, 1L, 0L, 0L, create777(), "owner", "group", new Path("hdfs", "hdfscluster:8020", "/test/testDir/someDir/1.out")));

        // first iteration will not pick up files because it has to instead check timestamps.
        // We must then wait long enough to ensure that the listing can be performed safely and
        // run the Processor again.
        runner.run();
        Thread.sleep(TimeUnit.NANOSECONDS.toMillis(2 * ListHDFS.LISTING_LAG_NANOS));
        runner.run();

        // assert that no files were listed
        runner.assertAllFlowFilesTransferred(ListHDFS.REL_SUCCESS, 0);
        // assert that no files were penalized
        runner.assertPenalizeCount(0);
    }

    private FsPermission create777() {
        return new FsPermission((short) 0777);
    }


    private class ListHDFSWithMockedFileSystem extends ListHDFS {
        private final MockFileSystem fileSystem = new MockFileSystem();
        private final KerberosProperties testKerberosProps;

        public ListHDFSWithMockedFileSystem(KerberosProperties kerberosProperties) {
            this.testKerberosProps = kerberosProperties;
        }

        @Override
        protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
            return testKerberosProps;
        }

        @Override
        protected FileSystem getFileSystem() {
            return fileSystem;
        }

        @Override
        protected File getPersistenceFile() {
            return new File("target/conf/state-file");
        }

        @Override
        protected FileSystem getFileSystem(final Configuration config) throws IOException {
            return fileSystem;
        }
    }

    private class MockFileSystem extends FileSystem {
        private final Map<Path, Set<FileStatus>> fileStatuses = new HashMap<>();

        public void addFileStatus(final Path parent, final FileStatus child) {
            Set<FileStatus> children = fileStatuses.get(parent);
            if (children == null) {
                children = new HashSet<>();
                fileStatuses.put(parent, children);
            }

            children.add(child);

            // if the child is directory and a key for it does not exist, create it with an empty set of children
            if (child.isDirectory() && !fileStatuses.containsKey(child.getPath())) {
                fileStatuses.put(child.getPath(), new HashSet<>());
            }
        }

        @Override
        @SuppressWarnings("deprecation")
        public long getDefaultBlockSize() {
            return 1024L;
        }

        @Override
        @SuppressWarnings("deprecation")
        public short getDefaultReplication() {
            return 1;
        }

        @Override
        public URI getUri() {
            return null;
        }

        @Override
        public FSDataInputStream open(final Path f, final int bufferSize) throws IOException {
            return null;
        }

        @Override
        public FSDataOutputStream create(final Path f, final FsPermission permission, final boolean overwrite, final int bufferSize, final short replication,
                                         final long blockSize, final Progressable progress) throws IOException {
            return null;
        }

        @Override
        public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
            return null;
        }

        @Override
        public boolean rename(final Path src, final Path dst) throws IOException {
            return false;
        }

        @Override
        public boolean delete(final Path f, final boolean recursive) throws IOException {
            return false;
        }

        @Override
        public FileStatus[] listStatus(final Path f) throws FileNotFoundException, IOException {
            return fileStatuses.keySet().stream()
                    // find the key in fileStatuses that matches the given Path f
                    .filter(pathKey -> f.isAbsoluteAndSchemeAuthorityNull()
                            // f is an absolute path with no scheme and no authority, compare with the keys of fileStatuses without their scheme and authority
                            ? Path.getPathWithoutSchemeAndAuthority(pathKey).equals(Path.getPathWithoutSchemeAndAuthority(f)) :
                            // f is absolute, but contains a scheme or authority, compare directly to the keys of fileStatuses
                            // if f is not absolute, false will be returned;
                            f.isAbsolute() && pathKey.equals(f))
                    // get the set of FileStatus objects for the filtered paths in the stream
                    .map(fileStatuses::get)
                    // return the first set of FileStatus objects in the stream; there should only be one, since fileStatuses is a Map
                    .findFirst()

                    // if no set of FileStatus objects was found, throw a FNFE
                    .orElseThrow(() -> new FileNotFoundException(String.format("%s instance does not contain an key for %s", this.getClass().getSimpleName(), f))).toArray(new FileStatus[0]);
        }

        @Override
        public void setWorkingDirectory(final Path new_dir) {

        }

        @Override
        public Path getWorkingDirectory() {
            return new Path(new File(".").getAbsolutePath());
        }

        @Override
        public boolean mkdirs(final Path f, final FsPermission permission) throws IOException {
            return false;
        }

        @Override
        public FileStatus getFileStatus(final Path f) throws IOException {
            return null;
        }

    }

    private class MockCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        private final ConcurrentMap<Object, Object> values = new ConcurrentHashMap<>();
        private boolean failOnCalls = false;

        private void verifyNotFail() throws IOException {
            if ( failOnCalls ) {
                throw new IOException("Could not call to remote service because Unit Test marked service unavailable");
            }
        }

        @Override
        public <K, V> boolean putIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            final Object retValue = values.putIfAbsent(key, value);
            return (retValue == null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V getAndPutIfAbsent(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer,
                final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.putIfAbsent(key, value);
        }

        @Override
        public <K> boolean containsKey(final K key, final Serializer<K> keySerializer) throws IOException {
            verifyNotFail();
            return values.containsKey(key);
        }

        @Override
        public <K, V> void put(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
            verifyNotFail();
            values.put(key, value);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <K, V> V get(final K key, final Serializer<K> keySerializer, final Deserializer<V> valueDeserializer) throws IOException {
            verifyNotFail();
            return (V) values.get(key);
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public <K> boolean remove(final K key, final Serializer<K> serializer) throws IOException {
            verifyNotFail();
            values.remove(key);
            return true;
        }

        @Override
        public long removeByPattern(String regex) throws IOException {
            verifyNotFail();
            final List<Object> removedRecords = new ArrayList<>();
            Pattern p = Pattern.compile(regex);
            for (Object key : values.keySet()) {
                // Key must be backed by something that array() returns a byte[] that can be converted into a String via the default charset
                Matcher m = p.matcher(key.toString());
                if (m.matches()) {
                    removedRecords.add(values.get(key));
                }
            }
            final long numRemoved = removedRecords.size();
            removedRecords.forEach(values::remove);
            return numRemoved;
        }
    }
}
