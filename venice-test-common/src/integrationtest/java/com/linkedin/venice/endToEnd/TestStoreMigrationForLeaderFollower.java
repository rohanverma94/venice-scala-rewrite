package com.linkedin.venice.endToEnd;

import com.linkedin.venice.integration.utils.MirrorMakerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiColoMultiClusterWrapper;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.*;


public class TestStoreMigrationForLeaderFollower extends TestStoreMigration {

  @Override
  protected VeniceTwoLayerMultiColoMultiClusterWrapper initializeVeniceCluster() {
    Properties parentControllerProperties = new Properties();
    // Disable topic cleanup since parent and child are sharing the same kafka cluster.
    parentControllerProperties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    // Required by metadata system store
    parentControllerProperties.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");

    Properties childControllerProperties = new Properties();
    // Required by metadata system store
    childControllerProperties.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "true");

    Properties serverProperties = new Properties();
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, 1L);

    // 1 parent controller, 1 child colo, 2 clusters per child colo, 2 servers per cluster
    // RF=2 to test both leader and follower SNs
    return ServiceFactory.getVeniceTwoLayerMultiColoMultiClusterWrapper(
        1,
        2,
        1,
        1,
        2,
        1,
        2,
        Optional.of(new VeniceProperties(parentControllerProperties)),
        Optional.of(childControllerProperties),
        Optional.of(new VeniceProperties(serverProperties)),
        true,
        MirrorMakerWrapper.DEFAULT_TOPIC_WHITELIST);
  }

  @Override
  protected boolean isLeaderFollowerModel() {
    return true;
  }
}