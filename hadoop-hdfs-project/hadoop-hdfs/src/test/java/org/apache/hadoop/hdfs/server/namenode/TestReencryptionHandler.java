/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import static org.junit.Assert.assertTrue;
/**
 * Test class for ReencryptionHandler.
 */
public class TestReencryptionHandler {

  @Rule
  public Timeout globalTimeout = new Timeout(120 * 1000);

  @Before
  public void setup() {
    GenericTestUtils.setLogLevel(ReencryptionHandler.LOG, Level.TRACE);
  }

  @Test
  public void testThrottle() throws Exception{
    final Configuration conf = new Configuration();
    conf.setDouble(
        DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_RATIO_KEY, 0.5);
    EncryptionZoneManager ezm = Mockito.mock(EncryptionZoneManager.class);
    ReencryptionHandler rh = new ReencryptionHandler(ezm, conf);
    StopWatch mockAll = Mockito.mock(StopWatch.class);
    Mockito.when(mockAll.now()).thenReturn(new Long(3000));
    Mockito.when(mockAll.reset()).thenReturn(mockAll);
    StopWatch mockLocked = Mockito.mock(StopWatch.class);
    Mockito.when(mockLocked.now()).thenReturn(new Long(2000));
    Mockito.when(mockLocked.reset()).thenReturn(mockLocked);
    Whitebox.setInternalState(rh, "throttleTimerAll", mockAll);
    Whitebox.setInternalState(rh, "throttleTimerLocked", mockLocked);
    StopWatch sw = new StopWatch().start();
    rh.throttle();
    assertTrue("should have throttled for at least 1 second",  sw.now() > 1000);
  }

  @Test(timeout = 180000)
  public void testThrottleDisabled() throws Exception{
    final Configuration conf = new Configuration();
    conf.setDouble(
        DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_RATIO_KEY, 10);
    EncryptionZoneManager ezm = Mockito.mock(EncryptionZoneManager.class);
    ReencryptionHandler rh = new ReencryptionHandler(ezm, conf);
    // all is way smaller than locked, verify test completes before
    // timeout (so throttle didn't happen)
    StopWatch mockAll = Mockito.mock(StopWatch.class);
    Mockito.when(mockAll.now()).thenReturn(new Long(3000));
    StopWatch mockLocked = Mockito.mock(StopWatch.class);
    Mockito.when(mockLocked.now()).thenReturn(new Long(180000));
    Whitebox.setInternalState(rh, "throttleTimerAll", mockAll);
    Whitebox.setInternalState(rh, "throttleTimerLocked", mockLocked);
    rh.throttle();
  }
}
