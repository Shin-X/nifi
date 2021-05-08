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
package org.apache.nifi.processors.standard.util;

import net.schmizz.sshj.sftp.Response;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockPropertyValue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestSFTPTransfer {

    private static final Logger logger = LoggerFactory.getLogger(TestSFTPTransfer.class);

    private SFTPTransfer createSftpTransfer(ProcessContext processContext, SFTPClient sftpClient) {
        final ComponentLog componentLog = mock(ComponentLog.class);
        return new SFTPTransfer(processContext, componentLog) {
            @Override
            protected SFTPClient getSFTPClient(FlowFile flowFile) throws IOException {
                return sftpClient;
            }
        };
    }

    @Test
    public void testEnsureDirectoryExistsAlreadyExisted() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsFailedToStat() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.FAILURE, "Failure"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Failed to determine if remote directory exists at /dir1/dir2/dir3 due to 4: Failure", e.getMessage());
        }

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3"));
    }

    @Test
    public void testEnsureDirectoryExistsNotExisted() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);
        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsParentNotExisted() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);

        // stat for the dir1 was successful, simulating that dir1 exists, but no dir2 and dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));
        when(sftpClient.stat("/dir1/dir2")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // dir2 was not found, too
        verify(sftpClient).stat(eq("/dir1")); // dir1 was found
        verify(sftpClient).mkdir(eq("/dir1/dir2")); // dir1 existed, so dir2 was created.
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // then dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsNotExistedFailedToCreate() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        final SFTPClient sftpClient = mock(SFTPClient.class);

        // stat for the parent was successful, simulating that dir2 exists, but no dir3.
        when(sftpClient.stat("/dir1/dir2/dir3")).thenThrow(new SFTPException(Response.StatusCode.NO_SUCH_FILE, "No such file"));
        // Failed to create dir3.
        doThrow(new SFTPException(Response.StatusCode.FAILURE, "Failed")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Failed to create remote directory /dir1/dir2/dir3 due to 4: Failed", e.getMessage());
        }

        // Dir existence check should be done by stat
        verify(sftpClient).stat(eq("/dir1/dir2/dir3")); // dir3 was not found
        verify(sftpClient).stat(eq("/dir1/dir2")); // so, dir2 was checked
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir2 existed, so dir3 was created.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyNotExisted() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyParentNotExisted() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        final AtomicInteger mkdirCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            final int cnt = mkdirCount.getAndIncrement();
            if (cnt == 0) {
                // If the parent dir does not exist, no such file exception is thrown.
                throw new SFTPException(Response.StatusCode.NO_SUCH_FILE, "Failure");
            } else {
                logger.info("Created the dir successfully for the 2nd time");
            }
            return true;
        }).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        // dir3 was created blindly, but failed for the 1st time, and succeeded for the 2nd time.
        verify(sftpClient, times(2)).mkdir(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2")); // dir2 was created successfully.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyAlreadyExisted() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        // If the dir existed, a failure exception is thrown, but should be swallowed.
        doThrow(new SFTPException(Response.StatusCode.FAILURE, "Failure")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

    @Test
    public void testEnsureDirectoryExistsBlindlyFailed() throws IOException, SFTPException {
        final ProcessContext processContext = mock(ProcessContext.class);
        when(processContext.getProperty(SFTPTransfer.DISABLE_DIRECTORY_LISTING)).thenReturn(new MockPropertyValue("true"));

        final SFTPClient sftpClient = mock(SFTPClient.class);
        doThrow(new SFTPException(Response.StatusCode.PERMISSION_DENIED, "Permission denied")).when(sftpClient).mkdir(eq("/dir1/dir2/dir3"));

        final SFTPTransfer sftpTransfer = createSftpTransfer(processContext, sftpClient);
        final MockFlowFile flowFile = new MockFlowFile(0);
        final File remoteDir = new File("/dir1/dir2/dir3");
        try {
            sftpTransfer.ensureDirectoryExists(flowFile, remoteDir);
            fail("Should fail");
        } catch (IOException e) {
            assertEquals("Could not blindly create remote directory due to Permission denied", e.getMessage());
        }

        // stat should not be called.
        verify(sftpClient, times(0)).stat(eq("/dir1/dir2/dir3"));
        verify(sftpClient).mkdir(eq("/dir1/dir2/dir3")); // dir3 was created blindly.
    }

}
