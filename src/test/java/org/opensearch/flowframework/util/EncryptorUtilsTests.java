/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.Config;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.opensearch.flowframework.common.CommonValue.CONFIG_INDEX;
import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.MASTER_KEY;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptorUtilsTests extends OpenSearchTestCase {

    private static final TestThreadPool testThreadPool = new TestThreadPool(
        EncryptorUtilsTests.class.getName(),
        new ScalingExecutorBuilder(
            WORKFLOW_THREAD_POOL,
            1,
            Math.max(2, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(1),
            FLOW_FRAMEWORK_THREAD_POOL_PREFIX + WORKFLOW_THREAD_POOL
        )
    );

    private ClusterService clusterService;
    private Client client;
    private SdkClient sdkClient;
    private NamedXContentRegistry xContentRegistry;
    private EncryptorUtils encryptorUtils;
    private String testMasterKey;
    private Template testTemplate;
    private String testCredentialKey;
    private String testCredentialValue;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(testThreadPool);
        this.xContentRegistry = mock(NamedXContentRegistry.class);
        this.sdkClient = SdkClientFactory.createSdkClient(client, xContentRegistry, Collections.emptyMap());
        this.encryptorUtils = new EncryptorUtils(clusterService, client, sdkClient, xContentRegistry);
        this.testMasterKey = encryptorUtils.generateMasterKey();
        this.testCredentialKey = "credential_key";
        this.testCredentialValue = "12345";

        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode(
            "A",
            "a-type",
            Collections.emptyMap(),
            Map.of(CREDENTIAL_FIELD, Map.of(testCredentialKey, testCredentialValue))
        );
        List<WorkflowNode> nodes = List.of(nodeA);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, Collections.emptyList());

        this.testTemplate = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("provision", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null,
            null
        );

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.hasIndex(anyString())).thenReturn(true);
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testGenerateMasterKey() {
        String generatedMasterKey = encryptorUtils.generateMasterKey();
        encryptorUtils.setMasterKey(null, generatedMasterKey);
        assertEquals(generatedMasterKey, encryptorUtils.getMasterKey(null));
    }

    public void testEncryptDecrypt() {
        encryptorUtils.setMasterKey(null, testMasterKey);
        String testString = "test";
        String encrypted = encryptorUtils.encrypt(testString, null);
        assertNotNull(encrypted);

        String decrypted = encryptorUtils.decrypt(encrypted, null);
        assertEquals(testString, decrypted);
    }

    public void testEncryptWithDifferentMasterKey() {
        encryptorUtils.setMasterKey(null, testMasterKey);
        String testString = "test";
        String encrypted1 = encryptorUtils.encrypt(testString, null);
        assertNotNull(encrypted1);

        // Change the master key prior to encryption
        String differentMasterKey = encryptorUtils.generateMasterKey();
        encryptorUtils.setMasterKey(null, differentMasterKey);
        String encrypted2 = encryptorUtils.encrypt(testString, null);

        assertNotEquals(encrypted1, encrypted2);
    }

    public void testInitializeMasterKeySuccess() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        String masterKey = encryptorUtils.generateMasterKey();

        // Index exists case
        // reinitialize with blank master key
        this.encryptorUtils = new EncryptorUtils(clusterService, client, sdkClient, xContentRegistry);
        assertNull(encryptorUtils.getMasterKey(null));

        GetResponse getMasterKeyResponse = TestHelpers.createGetResponse(new Config(masterKey, Instant.now()), MASTER_KEY, CONFIG_INDEX);
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onResponse(getMasterKeyResponse);
        when(client.get(any(GetRequest.class))).thenReturn(future);

        ActionListener<Boolean> listener = ActionListener.wrap(b -> {}, e -> {});

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<Boolean> latchedActionListener = new LatchedActionListener<>(listener, latch);
        encryptorUtils.initializeMasterKey(null, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);
        assertEquals(masterKey, encryptorUtils.getMasterKey(null));

        // Test ifAbsent version
        // reinitialize with blank master key
        this.encryptorUtils = new EncryptorUtils(clusterService, client, sdkClient, xContentRegistry);
        assertNull(encryptorUtils.getMasterKey(null));

        CompletableFuture<Void> resultFuture = encryptorUtils.initializeMasterKeyIfAbsent(null);
        resultFuture.get(5, TimeUnit.SECONDS);
        assertEquals(masterKey, encryptorUtils.getMasterKey(null));

        // No key exists case
        getMasterKeyResponse = TestHelpers.createGetResponse(null, MASTER_KEY, CONFIG_INDEX);
        future = PlainActionFuture.newFuture();
        future.onResponse(getMasterKeyResponse);
        when(client.get(any(GetRequest.class))).thenReturn(future);

        IndexResponse indexResponse = mock(IndexResponse.class);
        PlainActionFuture<IndexResponse> indexFuture = PlainActionFuture.newFuture();
        indexFuture.onResponse(indexResponse);
        when(client.index(any(IndexRequest.class))).thenReturn(indexFuture);

        listener = ActionListener.wrap(b -> {}, e -> {});
        latch = new CountDownLatch(1);
        latchedActionListener = new LatchedActionListener<>(listener, latch);
        encryptorUtils.initializeMasterKey(null, latchedActionListener);
        // This will generate a new master key 32 bytes -> base64 encoded
        latch.await(1, TimeUnit.SECONDS);
        assertNotEquals(masterKey, encryptorUtils.getMasterKey(null));
        assertEquals(masterKey.length(), encryptorUtils.getMasterKey(null).length());
    }

    public void testInitializeMasterKeyFailure() throws IOException {
        // reinitialize with blank master key
        this.encryptorUtils = new EncryptorUtils(clusterService, client, sdkClient, xContentRegistry);

        GetResponse getResponse = TestHelpers.createGetResponse(null, MASTER_KEY, CONFIG_INDEX);
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onResponse(getResponse);
        when(client.get(any(GetRequest.class))).thenReturn(future);

        CompletableFuture<Void> resultFuture = encryptorUtils.initializeMasterKeyIfAbsent(null);
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> resultFuture.get(5, TimeUnit.SECONDS));

        Throwable cause = executionException.getCause();
        assertNotNull(cause);
        assertTrue(cause instanceof FlowFrameworkException);
        FlowFrameworkException ffException = (FlowFrameworkException) cause;
        assertEquals("Master key has not been initialized in config index", ffException.getMessage());
        assertEquals(RestStatus.NOT_FOUND, ffException.status());
    }

    public void testEncryptDecryptTemplateCredential() {
        encryptorUtils.setMasterKey(null, testMasterKey);

        // Ecnrypt template with credential field
        Template processedtemplate = encryptorUtils.encryptTemplateCredentials(testTemplate);

        // Validate the encrytped field
        WorkflowNode node = processedtemplate.workflows().get("provision").nodes().get(0);

        @SuppressWarnings("unchecked")
        Map<String, String> encryptedCredentialMap = (Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD);
        assertEquals(1, encryptedCredentialMap.size());

        String encryptedCredential = encryptedCredentialMap.get(testCredentialKey);
        assertNotNull(encryptedCredential);
        assertNotEquals(testCredentialValue, encryptedCredential);

        // Decrypt credential field
        processedtemplate = encryptorUtils.decryptTemplateCredentials(processedtemplate);

        // Validate the decrypted field
        node = processedtemplate.workflows().get("provision").nodes().get(0);

        @SuppressWarnings("unchecked")
        Map<String, String> decryptedCredentialMap = (Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD);
        assertEquals(1, decryptedCredentialMap.size());

        String decryptedCredential = decryptedCredentialMap.get(testCredentialKey);
        assertNotNull(decryptedCredential);
        assertEquals(testCredentialValue, decryptedCredential);
    }

    public void testRedactTemplateCredential() {
        // Confirm credentials are present in the non-redacted template
        WorkflowNode node = testTemplate.workflows().get("provision").nodes().get(0);
        assertNotNull(node.userInputs().get(CREDENTIAL_FIELD));

        User user = new User("user", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

        // Redact template with credential field
        Template redactedTemplate = encryptorUtils.redactTemplateSecuredFields(user, testTemplate);

        // Validate the credential field has been removed
        WorkflowNode redactedNode = redactedTemplate.workflows().get("provision").nodes().get(0);
        assertNull(redactedNode.userInputs().get(CREDENTIAL_FIELD));
    }

    public void testRedactTemplateUserField() {
        // Confirm user is present in the non-redacted template
        assertNotNull(testTemplate.getUser());

        User user = new User("user", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        // Redact template with user field
        Template redactedTemplate = encryptorUtils.redactTemplateSecuredFields(user, testTemplate);

        // Validate the user field has been removed
        assertNull(redactedTemplate.getUser());
    }

    public void testAdminUserTemplate() {
        // Confirm user is present in the non-redacted template
        assertNotNull(testTemplate.getUser());

        List<String> roles = new ArrayList<>();
        roles.add("all_access");

        User user = new User("admin", roles, roles, Collections.emptyList());

        // Redact template with user field
        Template redactedTemplate = encryptorUtils.redactTemplateSecuredFields(user, testTemplate);
        assertNotNull(redactedTemplate.getUser());
    }
}
