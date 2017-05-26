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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.ReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.ReencryptionStatus.ZoneStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionFinalizer.FileEdekPair;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for handling re-encrypt EDEK operations.
 * <p>
 * This class uses the FSDirectory lock for synchronization.
 */
@InterfaceAudience.Private
public class ReencryptionHandler implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReencryptionHandler.class);

  private final EncryptionZoneManager ezManager;
  private final FSDirectory dir;
  private final ReencryptionStatus reencryptionStatus;
  private final long interval;
  private final int reencryptBatchSize;
  private double throttleLimitRatio;
  private final StopWatch throttleTimerAll = new StopWatch();
  final StopWatch throttleTimerLocked = new StopWatch();
  
  private CompletionService batchService;
  private final Map<Future<NavigableMap<String, FileEdekPair>>, INodesInPath>
      futures = new HashMap<>();
  private final Map<Long, List<Future>> ordering = new HashMap<>();
  private final ReencryptionFinalizer finalizer;
  private ExecutorService finalizerExecutor;

  // Vars for unit tests.
  private volatile boolean shouldPauseForTesting = false;
  private volatile int pauseAfterNthBatch = 0;
  private volatile boolean currentZoneCancelled = false;

  void stopThreads() {
    // TODO: multi-thread
    for (Future f : futures.keySet()) {
      f.cancel(true);
    }
    finalizerExecutor.shutdownNow();
  }
  
  void startThreads() {
    finalizerExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Re-encrypt EDEK Finalizer Thread #%d").build());
    finalizerExecutor.execute(finalizer);
  }

  public synchronized void pauseForTesting() {
    shouldPauseForTesting = true;
    LOG.info("Pausing re-encrypt thread for testing.");
    notify();
  }

  public synchronized void resumeForTesting() {
    shouldPauseForTesting = false;
    LOG.info("Resuming re-encrypt thread for testing.");
    notify();
  }

  void pauseForTestingAfterNthBatch(final int count) {
    assert pauseAfterNthBatch == 0;
    pauseAfterNthBatch = count;
  }

  private synchronized void checkPauseForTesting() throws InterruptedException {
    assert !dir.hasReadLock();
    assert !dir.getFSNamesystem().hasReadLock();
    while (shouldPauseForTesting) {
      LOG.info("Sleeping in the re-encrypt thread for unit test.");
      wait();
    }
    LOG.info("Continuing re-encrypt thread after pausing.");
  }

  ReencryptionHandler(final EncryptionZoneManager ezMgr,
      final Configuration conf) {
    this.ezManager = ezMgr;
    this.dir = ezMgr.getDir();
    reencryptionStatus = new ReencryptionStatus();
    this.interval =
        conf.getTimeDuration(DFSConfigKeys.DFS_NAMENODE_REENCRYPT_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_REENCRYPT_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    this.reencryptBatchSize =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY,
            DFSConfigKeys.DFS_NAMENODE_REENCRYPT_BATCH_SIZE_DEFAULT);
    this.throttleLimitRatio = conf.getDouble(
        DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_RATIO_KEY,
        DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_RATIO_DEFAULT);
    int num = 10;
    //TODO
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(1, num, 60, TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new Daemon.DaemonFactory() {
          private final AtomicInteger threadIndex = new AtomicInteger(0);

          @Override
          public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setName("finalizerRead-" + threadIndex.getAndIncrement());
            return t;
          }
        },
        new ThreadPoolExecutor.CallerRunsPolicy() {
          @Override
          public void rejectedExecution(Runnable runnable,
              ThreadPoolExecutor e) {
            LOG.info("Execution for rejected, executing in current thread");
            // will run in the current thread
            super.rejectedExecution(runnable, e);
          }
        });

    threadPool.allowCoreThreadTimeOut(true);
//    this.batchService = new ExecutorCompletionService(Executors.newFixedThreadPool(1));
    this.batchService = new ExecutorCompletionService(threadPool);
    finalizer = new ReencryptionFinalizer(dir, batchService, this);
  }

  Map<Long, List<Future>> getOrdering() {
    return ordering;
  }

  ReencryptionStatus getReencryptionStatus() {
    return reencryptionStatus;
  }

  void cancelCurrentZone() {
    currentZoneCancelled = true;
  }

  boolean isEnabled() {
    return throttleLimitRatio > 0;
  }

  INodesInPath getZoneFromFuture(Future future) {
    return futures.get(future);
  }
  
  void removeFuture(Future future) {
    futures.remove(future);
  }

  /**
   * Main loop. It takes at most 1 zone per scan, and executes until the zone
   * is completed.
   * {@see #reencryptEncryptionZoneInt(Long)}.
   */
  @Override
  public void run() {
    if (ezManager.getProvider() == null) {
      LOG.info("No provider set, cannot re-encrypt");
      return;
    }

    if (!isEnabled()) {
      LOG.info("Throttle limit set to {}, re-encrypt thread disabled.",
          throttleLimitRatio);
      return;
    }
    LOG.info("Starting up re-encrypt thread with interval={}.", interval);
    while (true) {
      try {
        Thread.sleep(interval);
        if (shouldPauseForTesting) {
          checkPauseForTesting();
        }
      } catch (InterruptedException ie) {
        LOG.info("re-encrypt thread interrupted.");
        Thread.currentThread().interrupt();
        return;
      }

      final Long zoneId;
      dir.readLock();
      try {
        zoneId = reencryptionStatus.getNextUnprocessedZone();
        if (zoneId == null) {
          // empty queue.
          continue;
        }
        currentZoneCancelled = false;
        LOG.info("Executing re-encrypt commands on zone {}. Current zones:{}",
            zoneId, reencryptionStatus);
      } finally {
        dir.readUnlock();
      }

      try {
        reencryptionStatus.markZoneStarted(zoneId);
        reencryptEncryptionZone(zoneId);
//        TODO: // Only complete a zone on success. Retry on failures indefinitely.
//        reencryptionStatus.removeZone(zoneId);
      } catch (IOException ioe) {
        LOG.warn("IOException caught when re-encrypting zone {}", zoneId, ioe);
      } catch (InterruptedException ie) {
        LOG.info("re-encrypt thread interrupted.");
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        LOG.error("Exception caught when re-encrypting zone {}.", zoneId, e);
        throw e;
      }
    }
  }

  /**
   * Re-encrypts a zone by recursively iterating all paths inside the zone,
   * in lexicographic order.
   * Files are re-encrypted, and subdirs are processed during iteration.
   *
   * @param zoneId the Zone's id.
   * @throws IOException
   * @throws InterruptedException
   */
  void reencryptEncryptionZone(final long zoneId)
      throws IOException, InterruptedException {
    throttleTimerAll.reset().start();
    throttleTimerLocked.reset();
    final NavigableMap<String, FileEdekPair> currentBatch = new TreeMap<>();
    final INode zoneNode;
    final INodesInPath zoneIIP;
    readLock();
    try {
      zoneNode = dir.getInode(zoneId);
      zoneIIP = INodesInPath.fromINode(zoneNode);
      // start re-encrypting the zone from the beginning
      if (zoneNode == null) {
        LOG.info("Directory with id {} removed during re-encrypt, skipping",
            zoneId);
        return;
      }
      if (!zoneNode.isDirectory()) {
        LOG.info("Cannot re-encrypt directory with id {} because it's not a"
            + " directory anymore.", zoneId);
        return;
      }
    } finally {
      readUnlock();
    }

    if (reencryptionStatus.getZoneStatus(zoneId).getLastProcessedFile() == null) {
      reencryptDir(zoneNode.asDirectory(), zoneIIP, HdfsFileStatus.EMPTY_NAME,
          currentBatch);
    } else {
      // resuming from a past re-encryption in this zone.
      restoreFromLastProcessedFile(zoneIIP, currentBatch);
    }
    // save the last batch and mark complete
    submitCurrentBatch(zoneIIP, currentBatch);
    // testing pause check has to happen outside of the lock.
    if (pauseAfterNthBatch > 0) {
      if (--pauseAfterNthBatch == 0) {
        shouldPauseForTesting = true;
        checkPauseForTesting();
      }
    }
    LOG.info(
        "Successfully submitted re-encryption of zone {} for async processing.",
        zoneIIP.getPath());
    finalizer.markZoneSubmissionDone(zoneIIP);
  }


  void completeReencryption(final INodesInPath zoneIIP)
      throws IOException, InterruptedException {
    List<XAttr> xAttrs;
    dir.getFSNamesystem().writeLock();
    try {
      dir.writeLock();
      try {
        final Long zoneId = zoneIIP.getLastINode().getId();
        ZoneStatus zs = reencryptionStatus.getZoneStatus(zoneId);
        LOG.info("Re-encryption completed on zone {}. Re-encrypted {} files,"
                + " failures encountered: {}.", zoneIIP.getPath(),
            zs.filesReencrypted, zs.filesReencryptionFailures);
        throttleTimerLocked.start();
        // This also removes the zone from reencryptionStatus
        xAttrs =
            FSDirEncryptionZoneOp.updateReencryptionFinish(dir, zoneIIP, false);
        finalizer.removeZone(zoneIIP);
      } finally {
        throttleTimerLocked.stop();
        dir.writeUnlock();
      }
      dir.getEditLog()
          .logSetXAttrs(zoneIIP.getPath(), xAttrs, true); //TODO retry cache
    } finally {
      dir.getFSNamesystem().writeUnlock("reencryptEDEK");
    }
  }

  /**
   * Restore the re-encryption from the progress inside ReencryptionStatus.
   * This means start from exactly the lastProcessedFile (LPF), skipping all
   * earlier paths in lexicographic order. Lexicographically-later directories
   * on the LPF parent paths are added to subdirs.
   */
  private void restoreFromLastProcessedFile(final INodesInPath zoneIIP,
      NavigableMap<String, FileEdekPair> currentBatch)
      throws IOException, InterruptedException {
    final INodesInPath lpfIIP = dir.getINodesInPath(
        reencryptionStatus.getZoneStatus(zoneIIP.getLastINode().getId())
            .getLastProcessedFile(),
            FSDirectory.DirOp.WRITE);
    // Walk through the path, from exactly the lpf, and recurse-out to all
    // parent dirs until the zone dir. Use LPF as startAfter.
    for (int i = lpfIIP.length() - 2; i > 0; --i) {
      final INode inode = lpfIIP.getINode(i);
      reencryptDir(lpfIIP.getINode(i).asDirectory(), zoneIIP,
          lpfIIP.getINode(i + 1).getLocalNameBytes(), currentBatch);
      // only recurse up to the zone.
      if (inode.getId() == zoneIIP.getLastINode().getId()) {
        break;
      }
    }
  }

  /**
   * Re-encrypts all files directly inside parent, and recurse down directories.
   * The listing is done in batch, and can optionally start after a position.
   * <p>
   * The re-encryption is also done in batch, and its progress is
   * only persisted after a full batch. This is tracked by the currentBatch
   * parameter.
   *
   * @param parent       The inode id of parent directory
   * @param zoneIIP      The encryption zone's INode id
   * @param startAfter   Full path of a file the re-encrypt should start after.
   * @param currentBatch Current re-encryption batch
   * @throws IOException
   * @throws InterruptedException
   */
  private void reencryptDir(final INodeDirectory parent, final INodesInPath zoneIIP,
      byte[] startAfter, NavigableMap<String, FileEdekPair> currentBatch)
      throws IOException, InterruptedException {
    readLock();
    try {
      reencryptDirInt(parent, zoneIIP, startAfter, currentBatch);
    } finally {
      readUnlock();
    }
  }

  /**
   * Submit the current batch to the thread pool, which will contact the KMS
   * for re-encryption.
   *
   * @param zoneIIP      The encryption zone's INode id
   * @param currentBatch The current batch
   * @throws IOException
   * @throws InterruptedException
   */
  private void submitCurrentBatch(final INodesInPath zoneIIP,
      final NavigableMap<String, FileEdekPair> currentBatch)
      throws IOException, InterruptedException {
    if (currentBatch.isEmpty()) {
      // TODO: status? handling (batch == 0)
//      completeReencryption(zoneIIP);
      return;
    }
    TreeMap<String, FileEdekPair> batch =
        (TreeMap<String, FileEdekPair>)
    ((TreeMap<String, FileEdekPair>) currentBatch).clone();
    final Long zoneId = zoneIIP.getLastINode().getId();
    Future future = batchService.submit(
        new EDEKReencryptCallable(zoneIIP, batch, ezManager.getProvider(),
            reencryptionStatus.getZoneStatus(zoneId)));
    futures.put(future, zoneIIP);
    if (ordering.get(zoneId) == null) {
      ordering.put(zoneId, new LinkedList<>());
    }
    ordering.get(zoneId).add(future);
    finalizer.incrementTaskCounter(zoneIIP);
    LOG.info("Submitted batch (start:{}, size:{}) of zone {} to re-encrypt.",
        batch.firstKey(), batch.size(), zoneIIP.getPath());
    currentBatch.clear();
  }
  
  private static class EDEKReencryptCallable implements Callable<NavigableMap<String, FileEdekPair>> {
    private final INodesInPath zoneIIP;
    private final NavigableMap<String, FileEdekPair> batch;
    private final KeyProviderCryptoExtension provider;
    private final ZoneStatus status;

    EDEKReencryptCallable(final INodesInPath iip,
        final NavigableMap<String, FileEdekPair> currentBatch,
        final KeyProviderCryptoExtension kp,
        final ZoneStatus zs) {
      zoneIIP = iip;
      batch = currentBatch;
      provider = kp;
      status = zs;
    }

    @Override
    public NavigableMap<String, FileEdekPair> call() throws Exception {
      LOG.info("Processing batched re-encryption for zone {}, batch size {}, "
          + "start:{}.", zoneIIP.getPath(), batch.size(), batch.firstKey());
      final Stopwatch kmsSW = new Stopwatch().start();
      // communicate with the kms out of lock
      for (Map.Entry<String, FileEdekPair> entry : batch.entrySet()) {
        try {
          final EncryptedKeyVersion edek =
              provider.reencryptEncryptedKey(entry.getValue().edek);
          if (edek == null) {
            LOG.debug("null EncryptedKeyVersion returned while re-encrypting {}",
                entry.getValue().edek);
            status.filesReencryptionFailures++;
            continue;
          }
          entry.getValue().edek = edek;
        } catch (GeneralSecurityException gse) {
          LOG.debug("Failed to re-encrypt {},", entry.getValue().edek, gse);
          status.filesReencryptionFailures++;
          continue;
        }
      }
      LOG.info("Completed re-encrypting one batch of {} edeks from KMS," 
              + " time consumed: {}, start:{}", batch.size(), kmsSW.stop(),
          batch.firstKey());
      return batch;
    }
  }

  /**
   * Iterates the parent directory, and add files to current batch. If batch
   * size meets configured threshold, a batch communication happens to the KMS
   * and file xattrs are updated upon return.
   *
   * @param parent       The inode id of parent directory
   * @param zoneIIP      The encryption zone's INode
   * @param startAfter   Full path of a file the re-encrypt should start after.
   * @param currentBatch Current re-encryption batch
   * @throws IOException
   * @throws InterruptedException
   */
  private void reencryptDirInt(final INodeDirectory parent, final INodesInPath zoneIIP,
      final byte[] startAfter,
      final NavigableMap<String, FileEdekPair> currentBatch)
      throws IOException, InterruptedException {
    assert dir.hasReadLock();
    assert dir.getFSNamesystem().hasReadLock();
    checkReadyForWrite();

    final ReadOnlyList<INode> children =
        parent.getChildrenList(Snapshot.CURRENT_STATE_ID);
    final String parentFullPath = parent.getFullPathName() + Path.SEPARATOR;
    LOG.debug("Re-encrypting directory {}", parentFullPath);
    for (int i = INodeDirectory.nextChild(children, startAfter);
         i < children.size(); ++i) {
      final INode inode = children.get(i);
      if (!reencryptINode(inode, zoneIIP, currentBatch)) {
        continue;
      }
      if (currentBatch.size() >= reencryptBatchSize) {
        readUnlock();
        try {
          submitCurrentBatch(zoneIIP, currentBatch);
          throttle();
          // testing pause check has to happen outside of the lock.
          if (pauseAfterNthBatch > 0) {
            if (--pauseAfterNthBatch == 0) {
              shouldPauseForTesting = true;
              checkPauseForTesting();
            }
          }
        } finally {
          readLock();
        }
        checkReadyForWrite();

        // Things could have changed when processing the last batch,
        // continue from last processed file.
        // If parent is deleted, FileEncryptionInfo will be null and
        // reencryptINode will take care of it.
        // -1 because of ++i from the for loop.
        i = INodeDirectory.nextChild(children, inode.getLocalNameBytes()) - 1;
      }
    }
  }

  private void readLock() {
    dir.getFSNamesystem().readLock();
    dir.readLock();
    throttleTimerLocked.start();
  }

  private void readUnlock() {
    throttleTimerLocked.stop();
    dir.readUnlock();
    dir.getFSNamesystem().readUnlock("reencryptEDEK");
  }

  @VisibleForTesting
  void throttle() throws InterruptedException {
    if (throttleLimitRatio >= 1.0) {
      return;
    }
    final long expect = (long) (throttleTimerAll.now() * throttleLimitRatio);
    final long actual = throttleTimerLocked.now();
    if (expect < actual) {
      final long interval = (long) ((actual - expect) / throttleLimitRatio);
      LOG.debug("Throttling re-encryption, sleeping for {} ms", interval);
      Thread.sleep(interval);
    }
    throttleTimerAll.reset().start();
    throttleTimerLocked.reset().start();
  }

  /**
   * Process an Inode for re-encryption. Add to current batch if it's a file,
   * recurse down if it's a dir.
   *
   * @param inode        the inode
   * @param zoneIIP      The encryption zone's INode id
   * @param currentBatch Current re-encryption batch
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  private boolean reencryptINode(final INode inode, final INodesInPath zoneIIP,
      final NavigableMap<String, FileEdekPair> currentBatch)
      throws IOException, InterruptedException {
    final String name = inode.getFullPathName();
    LOG.trace("Processing {} for re-encryption", name);
    if (inode.isDirectory()) {
      if (!ezManager.isEncryptionZoneRoot(inode)) {
        // always process the entire subdir
        reencryptDirInt(inode.asDirectory(), zoneIIP, HdfsFileStatus.EMPTY_NAME,
            currentBatch);
      } else {
        LOG.debug("{}({}) is a nested EZ, skipping for re-encrypt", name,
            inode.getId());
      }
      return false;
    }
    if (!inode.isFile()) {
      return false;
    }
    currentBatch.put(name, new FileEdekPair(dir, inode.asFile()));
    return true;
  }

  /**
   * Check whether the re-encryption thread is still ready to write.
   *
   * @throws IOException
   */
  void checkReadyForWrite() throws IOException {
    if (currentZoneCancelled) {
      throw new IOException("Current zone is cancelled.");
    }
    if (dir.getFSNamesystem().isInSafeMode()) {
      throw new IOException("NN is in safe mode.");
    }
  }
}