package com.linkedin.venice.hadoop;

import com.linkedin.venice.hadoop.SentPushJobDetailsTracker;
import com.linkedin.venice.status.protocol.PushJobDetails;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


public class SentPushJobDetailsTrackerImpl implements SentPushJobDetailsTracker {

  private final List<String> storeNames;
  private final List<Integer> versions;
  private final List<PushJobDetails> recordedPushJobDetails;

  SentPushJobDetailsTrackerImpl() {
    storeNames = new ArrayList<>();
    versions = new ArrayList<>();
    recordedPushJobDetails = new ArrayList<>();
  }

  @Override
  public void record(String storeName, int version, PushJobDetails pushJobDetails) {
    storeNames.add(storeName);
    versions.add(version);
    recordedPushJobDetails.add(makeCopyOf(pushJobDetails));
  }

  List<String> getStoreNames() {
    return Collections.unmodifiableList(storeNames);
  }

  List<Integer> getVersions() {
    return Collections.unmodifiableList(versions);
  }

  List<PushJobDetails> getRecordedPushJobDetails() {
    return Collections.unmodifiableList(recordedPushJobDetails);
  }

  private PushJobDetails makeCopyOf(PushJobDetails pushJobDetails) {
    PushJobDetails copyPushJobDetails = new PushJobDetails();
    copyPushJobDetails.reportTimestamp = pushJobDetails.reportTimestamp;
    copyPushJobDetails.overallStatus = new ArrayList<>(pushJobDetails.overallStatus);
    copyPushJobDetails.coloStatus = pushJobDetails.coloStatus == null ? pushJobDetails.coloStatus : new HashMap<>(pushJobDetails.coloStatus);
    copyPushJobDetails.pushId = pushJobDetails.pushId;
    copyPushJobDetails.partitionCount = pushJobDetails.partitionCount;
    copyPushJobDetails.valueCompressionStrategy = pushJobDetails.valueCompressionStrategy;
    copyPushJobDetails.chunkingEnabled = pushJobDetails.chunkingEnabled;
    copyPushJobDetails.jobDurationInMs = pushJobDetails.jobDurationInMs;
    copyPushJobDetails.totalNumberOfRecords = pushJobDetails.totalNumberOfRecords;
    copyPushJobDetails.totalKeyBytes = pushJobDetails.totalKeyBytes;
    copyPushJobDetails.totalRawValueBytes = pushJobDetails.totalRawValueBytes;
    copyPushJobDetails.pushJobConfigs = pushJobDetails.pushJobConfigs == null ? pushJobDetails.pushJobConfigs : new HashMap<>(pushJobDetails.pushJobConfigs);
    copyPushJobDetails.producerConfigs = pushJobDetails.producerConfigs == null ? pushJobDetails.producerConfigs : new HashMap<>(pushJobDetails.producerConfigs);
    copyPushJobDetails.pushJobLatestCheckpoint = pushJobDetails.pushJobLatestCheckpoint;
    copyPushJobDetails.failureDetails = pushJobDetails.failureDetails;
    return copyPushJobDetails;
  }
}
