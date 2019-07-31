package com.linkedin.venice.integration.utils;

import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.acl.StaticAccessController;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.listener.ListenerService;
import com.linkedin.venice.listener.StorageExecutionHandler;
import com.linkedin.venice.listener.request.RouterRequest;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.storage.DiskHealthCheckService;
import com.linkedin.venice.storage.MetadataRetriever;
import io.netty.channel.ChannelHandlerContext;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;


public class TestVeniceServer extends VeniceServer {
  public interface RequestHandler {
    boolean handleRequest(ChannelHandlerContext context, Object message);
  }

  private AtomicReference<RequestHandler> requestHandler = new AtomicReference<>();

  public TestVeniceServer(VeniceConfigLoader configLoader) throws VeniceException {
    super(configLoader);
  }

  public TestVeniceServer(
      VeniceConfigLoader configLoader,
      MetricsRepository metricsRepository,
      Optional<SSLEngineComponentFactory> sslFactory,
      Optional<StaticAccessController> accessController,
      Optional<ClientConfig> consumerClientConfig) {
    super(configLoader, metricsRepository, sslFactory, accessController, consumerClientConfig);
  }

  @Override
  protected ListenerService createListenerService(
      StoreRepository storeRepository,
      ReadOnlyStoreRepository storeMetadataRepository,
      ReadOnlySchemaRepository schemaRepository,
      CompletableFuture<RoutingDataRepository> routingRepository,
      MetadataRetriever metadataRetriever,
      VeniceServerConfig serverConfig,
      MetricsRepository metricsRepository,
      Optional<SSLEngineComponentFactory> sslFactory,
      Optional<StaticAccessController> accessController,
      DiskHealthCheckService diskHealthService) {

    return new ListenerService(
        storeRepository, storeMetadataRepository, schemaRepository, routingRepository, metadataRetriever, serverConfig,
        metricsRepository, sslFactory, accessController, diskHealthService) {

      @Override
      protected StorageExecutionHandler createRequestHandler(
          ExecutorService executor,
          ExecutorService computeExecutor,
          StoreRepository storeRepository,
          ReadOnlySchemaRepository schemaRepository,
          MetadataRetriever metadataRetriever,
          DiskHealthCheckService diskHealthService,
          boolean fastAvroEnabled) {

        return new StorageExecutionHandler(
            executor, computeExecutor, storeRepository, schemaRepository, metadataRetriever, diskHealthService, fastAvroEnabled) {
          @Override
          public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
            RequestHandler handler = requestHandler.get();
            if (handler == null || !handler.handleRequest(context, message)) {
              super.channelRead(context, message);
            }
          }
        };
      }

    };
  }

  public void setRequestHandler(RequestHandler handler) {
    requestHandler.set(handler);
  }
}