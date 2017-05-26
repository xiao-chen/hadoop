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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.ReencryptionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

/**
 * Class for finalizing re-encrypt EDEK operations.
 * <p>
 * This class uses the FSDirectory lock for synchronization. TODO
 */
public final class ReencryptionFinalizer implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReencryptionFinalizer.class);

  private final class ZoneSubmissionTracker {
    int pendingTasks;
    int totalTasks;
    boolean submissionDone;

    ZoneSubmissionTracker(final int count) {
      pendingTasks = count;
      totalTasks = count;
      submissionDone = false;
    }

    void incrTask(final int i) {
      pendingTasks += i;
      totalTasks += i;
    }

    void decsTask(final int i) {
      pendingTasks -= i;
    }
  }

  /**
   * Class that encapsulates re-encryption details of a file. It contains the
   * file inode, and stores the initial edek of the file. The edek is replaced
   * with the new version after re-encryption.
   * <p>
   * Assumptions are inode is valid and currently encrypted, and the object
   * initialization happens when dir lock is held.
   */
  static final class FileEdekPair {
    private INodeFile inodeFile;
    KeyProviderCryptoExtension.EncryptedKeyVersion edek = null;

    FileEdekPair(FSDirectory dir, INodeFile inode) throws IOException {
      inodeFile = inode;
      final FileEncryptionInfo fei = FSDirEncryptionZoneOp
          .getFileEncryptionInfo(dir, INodesInPath.fromINode(inode));
      Preconditions.checkNotNull(fei,
          "FileEncryptionInfo is null for " + inode.getFullPathName());
      edek = KeyProviderCryptoExtension.EncryptedKeyVersion
          .createForDecryption(fei.getKeyName(), fei.getEzKeyVersionName(),
              fei.getIV(), fei.getEncryptedDataEncryptionKey());
    }

    INodeFile getInodeFile() {
      return inodeFile;
    }
  }

  private final FSDirectory dir;
  private final CompletionService batchService;
  private final ReencryptionHandler handler;
  private final Map<Long, ZoneSubmissionTracker> perZoneTracker;

  ReencryptionFinalizer(final FSDirectory fsd, final CompletionService service,
      final ReencryptionHandler reencryptionHandler) {
    dir = fsd;
    batchService = service;
    handler = reencryptionHandler;
    perZoneTracker = new HashMap<>();
  }

  void incrementTaskCounter(final INodesInPath zoneIIP) {
    final ZoneSubmissionTracker tracker =
        perZoneTracker.get(zoneIIP.getLastINode().getId());
    if (tracker == null) {
      perZoneTracker
          .put(zoneIIP.getLastINode().getId(), new ZoneSubmissionTracker(1));
    } else {
      tracker.incrTask(1);
    }
  }

  void markZoneSubmissionDone(final INodesInPath zoneIIP)
      throws IOException, InterruptedException {
    final ZoneSubmissionTracker tracker =
        perZoneTracker.get(zoneIIP.getLastINode().getId());
    if (tracker != null) {
      tracker.submissionDone = true;
    } else {
      // Caller thinks submission is done, but no tasks submitted - meaning
      // no files in the EZ need to be re-encrypted. Complete directly.
      handler.completeReencryption(zoneIIP);
    }
  }

  void removeZone(final INodesInPath zoneIIP) {
    perZoneTracker.remove(zoneIIP.getLastINode().getId());
  }

  @Override
  public void run() {
    Set<Future> completedFutures = new HashSet<>();
    while (true) {
      try {
        processCompletedTasks(completedFutures);
      } catch (InterruptedException ie) {
        LOG.warn("Re-encryption finalizer thread interrupted.");
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        // TODO
        LOG.error("Re-encryption finalizer thread caught exception.", e);
      }
    }
  }

  private void processCompletedTasks(Set<Future> completedFutures)
      throws Exception {
    Future<NavigableMap<String, FileEdekPair>> completed =
        batchService.take();
    final INodesInPath zoneIIP = handler.getZoneFromFuture(completed);
    final Long zoneId = zoneIIP.getLastINode().getId();
    final Future<NavigableMap<String, FileEdekPair>> next =
        handler.getOrdering().get(zoneId).get(0);
    if (completed != next) {
      LOG.debug("Re-encryption finalizer: {} is completed, waiting on {}.",
          completed.get().firstKey(), next.get().firstKey());
      completedFutures.add(completed);
      return;
    }

    List<Future> currentZoneOrdering = handler.getOrdering().get(zoneId);
    // proceed all completed futures according to ordering
    Future<NavigableMap<String, FileEdekPair>> future;
    ReencryptionStatus.ZoneStatus status =
        handler.getReencryptionStatus().getZoneStatus(zoneId);
    future = currentZoneOrdering.get(0);
    do {
      // update NN within the write lock
      LOG.debug(
          "Updating file xattrs during re-encrypting zone {}, starting at {}",
          zoneIIP.getPath(), future.get().firstKey());
      String lastFile = null;
      dir.getFSNamesystem().writeLock();
      try {
        List<XAttr> xAttrs;
        dir.writeLock();
        try {
          handler.throttleTimerLocked.start();
          handler.checkReadyForWrite();
          for (Map.Entry<String, FileEdekPair> entry : future.get()
              .entrySet()) {
            final FileEdekPair rt = entry.getValue();
            INode now = dir.getINode(rt.inodeFile.getFullPathName(),
                FSDirectory.DirOp.WRITE);
            if (!Objects.equals(now, rt.inodeFile)) {
              // inode has changed, skip. The new inode should get a new edek:
              //    - renames are handled in another thread
              //    - new files gets new edeks automatically
              continue;
            }

            Preconditions.checkNotNull(rt.edek);
            final FileEncryptionInfo fei = FSDirEncryptionZoneOp
                .getFileEncryptionInfo(dir,
                    INodesInPath.fromINode(rt.inodeFile));
            FileEncryptionInfo newFei =
                new FileEncryptionInfo(fei.getCipherSuite(),
                    fei.getCryptoProtocolVersion(),
                    rt.edek.getEncryptedKeyVersion().getMaterial(),
                    rt.edek.getEncryptedKeyIv(), fei.getKeyName(),
                    rt.edek.getEncryptionKeyVersionName());
            FSDirEncryptionZoneOp.setFileEncryptionInfo(dir,
                INodesInPath.fromINode(rt.inodeFile), newFei,
                XAttrSetFlag.REPLACE);

            status.filesReencrypted++;
            //                reencryptionStatus.filesReencryptedTotal++;
            lastFile = entry.getKey();
          }

          xAttrs = Lists.newArrayListWithCapacity(1);
          LOG.info("Saving xattrs of re-encryption for {}, lastFile: {}",
              zoneIIP.getPath(), future.get().lastKey());
          FSDirEncryptionZoneOp.saveFileXAttrsForBatch(dir, future.get(),
              false); //TODO: retry cache

          final XAttr xattr = FSDirEncryptionZoneOp
              .updateReencryptionProgress(dir, zoneIIP, future.get());
          xAttrs.add(xattr);
          if (lastFile != null) {
            handler.getReencryptionStatus().getZoneStatus(zoneId)
                .setLastProcessedFile(lastFile);
          }
        } finally {
          handler.throttleTimerLocked.stop();
          dir.writeUnlock();
        }
        handler.removeFuture(future);
        dir.getEditLog()
            .logSetXAttrs(zoneIIP.getPath(), xAttrs, true); //TODO retry cache
      } finally {
        dir.getFSNamesystem().writeUnlock("reencryptEDEK");
      }
      dir.getEditLog().logSync();
      LOG.info(
          "Saved re-encrypt progress of zone {} to editlog, last processed"
              + " file is {}.", zoneIIP.getPath(), lastFile);
      final ZoneSubmissionTracker tracker =
          perZoneTracker.get(zoneIIP.getLastINode().getId());
      if (tracker.submissionDone && tracker.pendingTasks == 1) {
        LOG.debug("Zone {} completed in re-encryption finalizer. Total tasks"
            + " executed: {}.", zoneIIP.getPath(), tracker.totalTasks);
        handler.completeReencryption(zoneIIP);
      } else {
        tracker.decsTask(1);
      }
      completedFutures.remove(future);
      currentZoneOrdering.remove(future);
      if (!currentZoneOrdering.isEmpty()) {
        future = currentZoneOrdering.get(0);
      }
      // TODO: rework this damn loop
    } while (completedFutures.contains(future));
  }
}