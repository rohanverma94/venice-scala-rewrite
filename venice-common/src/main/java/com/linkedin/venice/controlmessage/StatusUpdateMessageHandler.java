package com.linkedin.venice.controlmessage;

import com.linkedin.venice.controlmessage.ControlMessageHandler;
import com.linkedin.venice.controlmessage.StatusUpdateMessage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;


/**
 * Handler in controller side used to deal with status update message from storage node.
 */
public class StatusUpdateMessageHandler implements ControlMessageHandler<StatusUpdateMessage> {
  private static final Logger logger = Logger.getLogger(ControlMessageHandler.class);
  //TODO will process the status in the further. Maybe here will become Map<KafkaTopic,Map<Partition,Map<Instance,Status>>>.
  private Map<String, StatusUpdateMessage> statusMap;

  public StatusUpdateMessageHandler() {
    statusMap = new ConcurrentHashMap<>();
  }

  @Override
  public void handleMessage(StatusUpdateMessage message) {
    logger.info("Processing message: "+message.getMessageId() + " For topic " + message.getKafkaTopic() );
    for (Map.Entry<String, String> entry : message.getFields().entrySet()) {
      logger.info(entry.getKey() + ":" + entry.getValue() + ";");
    }
    statusMap.put(message.getKafkaTopic(), message);
  }

  //TODO will be changed to get the status from kafkaTopic+partition+instance later.
  public StatusUpdateMessage getStatus(String kafkaTopic) {
    return statusMap.get(kafkaTopic);
  }
}
