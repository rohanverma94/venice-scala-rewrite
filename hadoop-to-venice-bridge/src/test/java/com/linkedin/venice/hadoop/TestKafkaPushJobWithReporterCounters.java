package com.linkedin.venice.hadoop;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.mapred.RunningJob;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestKafkaPushJobWithReporterCounters {

  @Test
  public void testHandleErrorsInCounter() throws IOException {
    KafkaPushJob kafkaPushJob = new KafkaPushJob("job-id", getH2VProps());
    kafkaPushJob.setControllerClient(createControllerClientMock());
    kafkaPushJob.setJobClientWrapper(createJobClientWrapperMock());
    kafkaPushJob.setClusterDiscoveryControllerClient(createClusterDiscoveryControllerClientMock());
    kafkaPushJob.run();
  }

  private Properties getH2VProps() {
    Properties props = new Properties();
    props.put(KafkaPushJob.VENICE_URL_PROP, "venice-urls");
    props.put(KafkaPushJob.VENICE_STORE_NAME_PROP, "store-name");
    props.put(KafkaPushJob.INPUT_PATH_PROP, "input-path");
    props.put(KafkaPushJob.KEY_FIELD_PROP, "id");
    props.put(KafkaPushJob.VALUE_FIELD_PROP, "name");
    // No need for a big close timeout in tests. This is just to speed up discovery of certain regressions.
    props.put(VeniceWriter.CLOSE_TIMEOUT_MS, 500);
    props.put(KafkaPushJob.POLL_JOB_STATUS_INTERVAL_MS, 1000);
    props.setProperty(KafkaPushJob.SSL_KEY_STORE_PROPERTY_NAME, "test");
    props.setProperty(KafkaPushJob.SSL_TRUST_STORE_PROPERTY_NAME,"test");
    props.setProperty(KafkaPushJob.SSL_KEY_STORE_PASSWORD_PROPERTY_NAME,"test");
    props.setProperty(KafkaPushJob.SSL_KEY_PASSWORD_PROPERTY_NAME,"test");
    props.setProperty(KafkaPushJob.PUSH_JOB_STATUS_UPLOAD_ENABLE, "false");
    return props;
  }

  private ControllerClient createClusterDiscoveryControllerClientMock() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    D2ServiceDiscoveryResponse controllerResponse = mock(D2ServiceDiscoveryResponse.class);
    when(controllerResponse.getCluster()).thenReturn("mock-cluster-name");
    when(controllerClient.discoverCluster(anyString())).thenReturn(controllerResponse);
    return controllerClient;
  }

  private ControllerClient createControllerClientMock() {
    ControllerClient controllerClient = mock(ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class);

    StorageEngineOverheadRatioResponse storageEngineOverheadRatioResponse = mock(StorageEngineOverheadRatioResponse.class);
    when(storageEngineOverheadRatioResponse.isError()).thenReturn(false);
    when(storageEngineOverheadRatioResponse.getStorageEngineOverheadRatio()).thenReturn(1.0);

    when(storeInfo.getStorageQuotaInByte()).thenReturn(1000L);
    when(storeInfo.isSchemaAutoRegisterFromPushJobEnabled()).thenReturn(false);
    when(storeResponse.getStore()).thenReturn(storeInfo);

    when(controllerClient.getStore(anyString())).thenReturn(storeResponse);
    when(controllerClient.getStorageEngineOverheadRatio(anyString())).thenReturn(storageEngineOverheadRatioResponse);

    return controllerClient;
  }

  private JobClientWrapper createJobClientWrapperMock() throws IOException {
    RunningJob runningJob = mock(RunningJob.class);
    JobClientWrapper jobClientWrapper = mock(JobClientWrapper.class);
    when(jobClientWrapper.runJobWithConfig(any())).thenReturn(runningJob);
    return jobClientWrapper;
  }
}
