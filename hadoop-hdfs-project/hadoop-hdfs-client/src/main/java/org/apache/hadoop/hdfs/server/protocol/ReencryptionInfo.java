/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Reencryption information for an encryption zone.
 */
@InterfaceAudience.Private
public class ReencryptionInfo {

  private final String ezKeyVersionName;
  private final long submissionTime;
  private boolean isCanceled;
  private long completionTime;
  private String lastFile;

  public ReencryptionInfo(final String ezkvn, final long submissiionT,
      final boolean canceled, final long completionT, final String file) {
    ezKeyVersionName = ezkvn;
    submissionTime = submissiionT;
    isCanceled = canceled;
    completionTime = completionT;
    lastFile = file;
  }

  public void setLastFile(final String file) {
    lastFile = file;
  }

  public long getSubmissionTime() {
    return submissionTime;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    builder.append("submission time: " + submissionTime);
    builder.append(", last file: " + lastFile);
    builder.append("}");
    return builder.toString();
  }
}
