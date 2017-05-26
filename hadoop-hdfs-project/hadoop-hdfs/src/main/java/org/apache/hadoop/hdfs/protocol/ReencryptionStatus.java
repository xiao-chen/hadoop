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
package org.apache.hadoop.hdfs.protocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A class representing information about re-encrypting encryption zones. It
 * contains the cursor to the last processed file in each zone currently under
 * re-encryption, as well as a queue of zones that are pending re-encryption.
 */
@InterfaceAudience.Private
public final class ReencryptionStatus {

  public static Logger LOG = LoggerFactory.getLogger(ReencryptionStatus.class);

  public final class ZoneStatus {
    private State state;
    /**
     * Name of last file processed. It's important to record name (not inode)
     * because we want to restore to the position even if the inode is removed.
     */
    private String lastProcessedFile;
    public long filesReencrypted;
    public long filesReencryptionFailures;
    
    ZoneStatus() {
      reset();
    }
    
    public void resetMetrics() {
      filesReencrypted = 0;
      filesReencryptionFailures = 0;
    }
    
    public void markCompleted() {
      state = State.Completed;
      lastProcessedFile = null;
    }

    public State getState() {
      return state;
    }

    public void setLastProcessedFile(final String file) {
      lastProcessedFile = file;
    }
    
    public String getLastProcessedFile() {
      return lastProcessedFile;
    }

    public void reset() {
      state = State.Submitted;
      lastProcessedFile = null;
      resetMetrics();
    }
  }
  
  public enum State {
    /**
     * Submitted for re-encryption but hasn't been picked up.
     * This is the initial state.
     */
    Submitted,
    /**
     * Currently re-encrypting.
     */
    Processing,
    /**
     * Re-encryption completed.
     */
    Completed
  }

  /**
   * The zones that were submitted for re-encryption. This should preserve
   * the order of submission.
   */
  private final LinkedHashMap<Long, ZoneStatus> reencryptRequests;

  // Metrics
  public long zonesReencrypted;

  public ReencryptionStatus() {
    reencryptRequests = new LinkedHashMap<>();
  }

  @VisibleForTesting
  public ReencryptionStatus(ReencryptionStatus rhs) {
    if (rhs != null) {
      synchronized (rhs) {
        this.reencryptRequests = new LinkedHashMap<>(rhs.reencryptRequests);
        this.zonesReencrypted = rhs.zonesReencrypted;
      }
    } else {
      reencryptRequests = new LinkedHashMap<>();
    }
  }

  public synchronized void resetMetrics() {
    zonesReencrypted = 0;
    for (Map.Entry<Long, ZoneStatus> entry : reencryptRequests.entrySet()) {
      entry.getValue().resetMetrics();
    }
  }

  public synchronized void resetMetricsForZone(final Long zondId) {
    ZoneStatus p = reencryptRequests.get(zondId);
    if (p != null) {
      p.resetMetrics();
    }
  }
  
  public synchronized ZoneStatus getZoneStatus(final Long zondId) {
    return reencryptRequests.get(zondId);
  }

  public synchronized void markZoneStarted(final Long zoneId) {
    final ZoneStatus zs = reencryptRequests.get(zoneId);
    Preconditions.checkNotNull(zs, "Cannot find zone " + zoneId);
    LOG.debug("Zone {} starts re-encryption processing", zoneId);
    zs.state = State.Processing;
  }

  public synchronized void markZoneCompleted(final Long zoneId) {
    final ZoneStatus zs = reencryptRequests.get(zoneId);
    Preconditions.checkNotNull(zs, "Cannot find zone " + zoneId);
    zs.markCompleted();
    LOG.debug("Zone {} re-encryption completed. Files re-encrypted: {}, "
            + "failures met {}", zoneId, zs.filesReencrypted,
        zs.filesReencryptionFailures);
    zonesReencrypted++;
  }

  public synchronized Long getNextUnprocessedZone() {
    for (Map.Entry<Long, ZoneStatus> entry : reencryptRequests.entrySet()) {
      if (entry.getValue().state == State.Submitted) {
        return entry.getKey();
      }
    }
    return null;
  }

  public synchronized boolean hasZone(final Long zoneId) {
    return reencryptRequests.containsKey(zoneId)
        && reencryptRequests.get(zoneId).state != State.Completed;
  }

  /**
   * 
   * @param zoneId
   * @return true if this is a zone is added.
   */
  public synchronized boolean addZoneIfNecessary(final Long zoneId) {
    if (!reencryptRequests.containsKey(zoneId)) {
      LOG.debug("Adding zone {} for re-encryption status", zoneId);
      return reencryptRequests.put(zoneId, new ZoneStatus()) == null;
    }
    return false;
  }

  /**
   * 
   * @param zoneId
   * @return true if zone is found
   */
  public synchronized boolean removeZone(final Long zoneId) {
    LOG.debug("Removing re-encryption status of zone {} ", zoneId);
    return reencryptRequests.remove(zoneId) != null;
  }

  @VisibleForTesting
  public synchronized int zonesQueued() {
    int ret = 0;
    for (Map.Entry<Long, ZoneStatus> entry : reencryptRequests.entrySet()) {
      if (entry.getValue().state == State.Submitted) {
        ret++;
      }
    }
    return ret;
  }

  @VisibleForTesting
  public synchronized int zonesTotal() {
    return reencryptRequests.size();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    synchronized (this) {
      for (Map.Entry<Long, ZoneStatus> entry : reencryptRequests.entrySet()) {
        sb.append("[zone:" + entry.getKey());
        sb.append(" state:" + entry.getValue().state);
        sb.append(" lastProcessed:" + entry.getValue().lastProcessedFile + "]");
      }
    }
    return sb.toString();
  }
}