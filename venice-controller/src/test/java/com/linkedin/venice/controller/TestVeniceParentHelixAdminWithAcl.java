package com.linkedin.venice.controller;

import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Principal;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.TestUtils;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * This contains unit test cases for venice acl authorizer class that is used during store creation.
 * Creating a new class as we need to pass MockAuthorizer during cluster setup.
 */
public class TestVeniceParentHelixAdminWithAcl extends AbstractTestVeniceParentHelixAdmin {

  MockVeniceAuthorizer authorizerService;

  @BeforeMethod
  public void setup() throws Exception {
    authorizerService = new MockVeniceAuthorizer();
    setupTestCase(Optional.of(authorizerService));
  }

  @AfterMethod
  public void cleanupTestCase() {
    super.cleanupTestCase();
  }

  /**
   * This tests if addStore() is able to properly construct the AclBinding object from input json.
   */
  @Test
  public void testStoreCreationWithAuthorization() {
    String storeName = "test-store-authorizer";
    String accessPerm =
        "{\"AccessPermissions\":{\"Read\":[\"urn:li:corpuser:user1\",\"urn:li:corpGroup:group1\",\"urn:li:servicePrincipal:app1\"],\"Write\":[\"urn:li:corpuser:user1\",\"urn:li:corpGroup:group1\",\"urn:li:servicePrincipal:app1\"]}}";

    AclBinding expectedAB = new AclBinding(new Resource(storeName));
    Principal userp = new Principal("urn:li:corpuser:user1");
    Principal groupp = new Principal("urn:li:corpGroup:group1");
    Principal servicep = new Principal("urn:li:servicePrincipal:app1");
    for (Principal p : new Principal[]{userp, groupp, servicep}) {
      AceEntry race = new AceEntry(p, Method.Read, Permission.ALLOW);
      AceEntry wace = new AceEntry(p, Method.Write, Permission.ALLOW);
      expectedAB.addAceEntry(race);
      expectedAB.addAceEntry(wace);
    }

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1))).when(
        veniceWriter).put(any(), any(), anyInt());

    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.start(clusterName);
    parentAdmin.addStore(clusterName, storeName, "dev", keySchemaStr, valueSchemaStr, false, Optional.of(accessPerm));
    Assert.assertEquals(1, authorizerService.setAclsCounter);
    AclBinding actualAB = authorizerService.describeAcls(new Resource(storeName));
    Assert.assertTrue(isAclBindingSame(expectedAB, actualAB));
  }

  /**
   * This tests if addStore() is able to throw exception and and stop further processing when acl provisioning fails.
   */
  @Test
  public void testStoreCreationWithAuthorizationException() {
    String storeName = "test-store-authorizer";
    //send an invalid json, so that parsing this would generate an exception and thus failing the addStore api to move forward.
    String accessPerm =
        "{\"AccessPermissions\":{\"Read\":[\"urn:li:corpuser:user1\",\"urn:li:corpGroup:group1\",\"urn:li:servicePrincipal:app1\"],\"Write\":[\"urn:li:corpuser:user1\",\"urn:li:corpGroup:group1\",\"urn:li:servicePrincipal:app1\"],}}";

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1))).when(
        veniceWriter).put(any(), any(), anyInt());

    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    parentAdmin.start(clusterName);
    Assert.assertThrows(VeniceException.class,
        () -> parentAdmin.addStore(clusterName, storeName, "dev", keySchemaStr, valueSchemaStr, false,
            Optional.of(accessPerm)));
    Assert.assertEquals(0, authorizerService.setAclsCounter);

  }

  @Test
  public void testDeleteStoreWithAuthorization() {
    String storeName = "test-store-authorizer-delete";
    String owner = "unittest";
    Store store = TestUtils.createTestStore(storeName, owner, System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(eq(clusterName), eq(storeName));
    doReturn(store).when(internalAdmin).checkPreConditionForDeletion(eq(clusterName), eq(storeName));

    doReturn(CompletableFuture.completedFuture(new RecordMetadata(topicPartition, 0, 1, -1, -1L, -1, -1)))
        .when(veniceWriter).put(any(), any(), anyInt());

    when(zkClient.readData(zkMetadataNodePath, null))
        .thenReturn(null)
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    parentAdmin.start(clusterName);
    parentAdmin.deleteStore(clusterName, storeName, 0);
    Assert.assertEquals(1, authorizerService.clearAclCounter);
    AclBinding actualAB = authorizerService.describeAcls(new Resource(storeName));
    Assert.assertTrue(isAclBindingSame(new AclBinding(new Resource(storeName)), actualAB));
  }

  /**
   * This tests if updateAcl() is able to update the ACl rules for a store and deleteAcl is able to delete it.
   */
  @Test
  public void testUpdateAndGetAndDeleteAcl() {
    String storeName = "test-store-authorizer";
    String expectedPerm =
        "{\"AccessPermissions\":{\"Read\":[\"urn:li:corpuser:user2\",\"urn:li:corpGroup:group2\",\"urn:li:servicePrincipal:app2\"],\"Write\":[\"urn:li:corpuser:user2\",\"urn:li:corpGroup:group2\",\"urn:li:servicePrincipal:app2\"]}}";
    parentAdmin.updateAclForStore(clusterName, storeName, expectedPerm);
    Assert.assertEquals(1, authorizerService.setAclsCounter);
    String curPerm = parentAdmin.getAclForStore(clusterName, storeName);
    Assert.assertEquals(expectedPerm, curPerm);

    parentAdmin.deleteAclForStore(clusterName, storeName);
    Assert.assertEquals(1, authorizerService.clearAclCounter);
    AclBinding actualAB = authorizerService.describeAcls(new Resource(storeName));
    Assert.assertTrue(isAclBindingSame(new AclBinding(new Resource(storeName)), actualAB));
    curPerm = parentAdmin.getAclForStore(clusterName, storeName);
    Assert.assertEquals(curPerm, "");
  }

  /**
   * This test exceptions are properly thrown when a updateacl called for non-existent store.
   */
  @Test
  public void testUpdateAclException() {
    String storeName = "test-store-authorizer";
    String expectedPerm =
        "{\"AccessPermissions\":{\"Read\":[\"urn:li:corpuser:user2\",\"urn:li:corpGroup:group2\",\"urn:li:servicePrincipal:app2\"],\"Write\":[\"urn:li:corpuser:user2\",\"urn:li:corpGroup:group2\",\"urn:li:servicePrincipal:app2\"]}}";

    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin)
        .checkPreConditionForAclOp(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(new OffsetRecord())
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    Assert.assertThrows(VeniceNoStoreException.class,
        () -> parentAdmin.updateAclForStore(clusterName, storeName, expectedPerm));
    Assert.assertEquals(0, authorizerService.setAclsCounter);
  }

  /**
   * This test exceptions are properly thrown when a getAcl called for non-existent store.
   */
  @Test
  public void testGetAclException() {
    String storeName = "test-store-authorizer";
    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin)
        .checkPreConditionForAclOp(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(new OffsetRecord())
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    Assert.assertThrows(VeniceNoStoreException.class, () -> parentAdmin.getAclForStore(clusterName, storeName));
  }

  /**
   * This test exceptions are properly thrown when a deleteAcl called for non-existent store.
   */
  @Test
  public void testDeleteAclException() {
    String storeName = "test-store-authorizer";
    doThrow(new VeniceNoStoreException(storeName)).when(internalAdmin)
        .checkPreConditionForAclOp(clusterName, storeName);

    when(zkClient.readData(zkMetadataNodePath, null)).thenReturn(new OffsetRecord())
        .thenReturn(AdminTopicMetadataAccessor.generateMetadataMap(1, 1));

    Assert.assertThrows(VeniceNoStoreException.class, () -> parentAdmin.deleteAclForStore(clusterName, storeName));
    Assert.assertEquals(0, authorizerService.clearAclCounter);

  }

  private boolean isAclBindingSame(AclBinding ab1, AclBinding ab2) {
    if (!ab1.getResource().equals(ab2.getResource())) {
      return false;
    }

    Collection<AceEntry> aces1 = ab1.getAceEntries();
    Collection<AceEntry> aces2 = ab2.getAceEntries();

    if (aces1.size() != aces2.size()) {
      return false;
    }

    for (AceEntry e1 : aces1) {
      boolean match = false;
      for (AceEntry e2 : aces2) {
        if (e1.equals(e2)) {
          match = true;
          break;
        }
      }
      if (!match) {
        return false;
      }
    }
    return true;
  }
}