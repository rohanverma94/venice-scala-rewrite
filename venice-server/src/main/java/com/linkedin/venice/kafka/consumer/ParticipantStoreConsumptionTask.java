package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.participant.protocol.KillPushJob;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.ParticipantMessageStoreUtils;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.participant.protocol.enums.ParticipantMessageType;
import com.linkedin.venice.stats.ParticipantStoreConsumptionStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


public class ParticipantStoreConsumptionTask implements Runnable, Closeable {

  private static RedundantExceptionFilter filter = RedundantExceptionFilter.getRedundantExceptionFilter();

  private final Logger logger = Logger.getLogger(ParticipantStoreConsumptionTask.class);
  private final String clusterName;
  private final ParticipantStoreConsumptionStats stats;
  private final StoreIngestionService storeIngestionService;
  private final AtomicBoolean isRunning = new AtomicBoolean(true);
  private final long participantMessageConsumptionDelayMs;
  private AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> participantStoreClient;

  public ParticipantStoreConsumptionTask(String clusterName, StoreIngestionService storeIngestionService,
      ParticipantStoreConsumptionStats stats, ClientConfig<ParticipantMessageValue> clientConfig,
      long participantMessageConsumptionDelayMs) {
    this.clusterName = clusterName;
    this.stats =  stats;
    this.storeIngestionService = storeIngestionService;
    this.participantMessageConsumptionDelayMs = participantMessageConsumptionDelayMs;
    String participantStoreName = ParticipantMessageStoreUtils.getStoreNameForCluster(clusterName);
    clientConfig.setStoreName(participantStoreName);
    clientConfig.setSpecificValueClass(ParticipantMessageValue.class);
    try {
      participantStoreClient = ClientFactory.getAndStartSpecificAvroClient(clientConfig);
    } catch (Exception e) {
      isRunning.set(false);
      stats.recordFailedInitialization();
      logger.error("Failed to initialize participant message consumption task because participant client can't"
          + " be started", e);
    }
  }
  @Override
  public void close() throws IOException {
    isRunning.set(false);
    IOUtils.closeQuietly(participantStoreClient);
  }

  @Override
  public void run() {
    logger.info("Started running " + getClass().getSimpleName() + " thread for cluster: " + clusterName);
    String exceptionMessage = "Exception thrown while running " + getClass().getSimpleName() + " thread";
    while (isRunning.get()) {
      try {
        Utils.sleep(participantMessageConsumptionDelayMs);
        ParticipantMessageKey key = new ParticipantMessageKey();
        key.messageType = ParticipantMessageType.KILL_PUSH_JOB.getValue();
        for (String topic : storeIngestionService.getIngestingTopics()) {
          key.resourceName = topic;
          ParticipantMessageValue value = participantStoreClient.get(key).get();
          if (value != null && value.messageType == ParticipantMessageType.KILL_PUSH_JOB.getValue()) {
            KillPushJob killPushJobMessage = (KillPushJob) value.messageUnion;
            storeIngestionService.killConsumptionTask(topic);
            stats.recordKilledPushJobs();
            stats.recordKillPushJobLatency(Long.max(0,System.currentTimeMillis() - killPushJobMessage.timestamp));
          }
        }
      }
      catch (Exception e) {
        // Some expected exception can be thrown during initializing phase of the participant store or if participant
        // store is disabled. TODO: remove logging suppression based on message once we fully move to participant store
        if (!filter.isRedundantException(e.getMessage())) {
          logger.error(exceptionMessage, e);
        }
        stats.recordKillPushJobFailedConsumption();
      }
    }
    logger.info("Stopped running " + getClass().getSimpleName() + " thread for cluster: " + clusterName);
  }
}