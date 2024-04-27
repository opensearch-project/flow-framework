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
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.Config;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptorUtilsTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private Client client;
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
        this.client = mock(Client.class);
        this.xContentRegistry = mock(NamedXContentRegistry.class);
        this.encryptorUtils = new EncryptorUtils(clusterService, client, xContentRegistry);
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
            null
        );

        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.hasIndex(anyString())).thenReturn(true);

        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
    }

    public void testGenerateMasterKey() {
        String generatedMasterKey = encryptorUtils.generateMasterKey();
        encryptorUtils.setMasterKey(generatedMasterKey);
        assertEquals(generatedMasterKey, encryptorUtils.getMasterKey());
    }

    public void testEncryptDecrypt() {
        encryptorUtils.setMasterKey(testMasterKey);
        String testString = "test";
        String encrypted = encryptorUtils.encrypt(testString);
        assertNotNull(encrypted);

        String decrypted = encryptorUtils.decrypt(encrypted);
        assertEquals(testString, decrypted);
    }

    public void testEncryptWithDifferentMasterKey() {
        encryptorUtils.setMasterKey(testMasterKey);
        String testString = "test";
        String encrypted1 = encryptorUtils.encrypt(testString);
        assertNotNull(encrypted1);

        // Change the master key prior to encryption
        String differentMasterKey = encryptorUtils.generateMasterKey();
        encryptorUtils.setMasterKey(differentMasterKey);
        String encrypted2 = encryptorUtils.encrypt(testString);

        assertNotEquals(encrypted1, encrypted2);
    }

    public void testInitializeMasterKeySuccess() throws IOException {
        encryptorUtils.setMasterKey(null);

        String masterKey = encryptorUtils.generateMasterKey();

        // Index exists case
        BytesReference bytesRef;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            Config config = new Config(masterKey, Instant.now());
            XContentBuilder source = config.toXContent(builder, ToXContent.EMPTY_PARAMS);
            bytesRef = BytesReference.bytes(source);
        }
        doAnswer(invocation -> {
            ActionListener<GetResponse> getRequestActionListener = invocation.getArgument(1);

            // Stub get response for success case
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(true);
            when(getResponse.getSourceAsBytesRef()).thenReturn(bytesRef);

            getRequestActionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        ActionListener<Boolean> listener = ActionListener.wrap(b -> {}, e -> {});
        encryptorUtils.initializeMasterKey(listener);
        assertEquals(masterKey, encryptorUtils.getMasterKey());

        // Test ifAbsent version
        encryptorUtils.setMasterKey(null);
        assertNull(encryptorUtils.getMasterKey());

        encryptorUtils.initializeMasterKeyIfAbsent();
        assertEquals(masterKey, encryptorUtils.getMasterKey());

        // No index exists case
        doAnswer(invocation -> {
            ActionListener<GetResponse> getRequestActionListener = invocation.getArgument(1);
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(false);
            getRequestActionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());
        doAnswer(invocation -> {
            ActionListener<IndexResponse> indexRequestActionListener = invocation.getArgument(1);
            IndexResponse indexResponse = mock(IndexResponse.class);
            indexRequestActionListener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any());

        listener = ActionListener.wrap(b -> {}, e -> {});
        encryptorUtils.initializeMasterKey(listener);
        // This will generate a new master key 32 bytes -> base64 encoded
        assertNotEquals(masterKey, encryptorUtils.getMasterKey());
        assertEquals(masterKey.length(), encryptorUtils.getMasterKey().length());
    }

    public void testInitializeMasterKeyFailure() {
        encryptorUtils.setMasterKey(null);

        doAnswer(invocation -> {
            ActionListener<GetResponse> getRequestActionListener = invocation.getArgument(1);

            // Stub get response for failure case
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(false);
            getRequestActionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        FlowFrameworkException ex = expectThrows(FlowFrameworkException.class, () -> encryptorUtils.initializeMasterKeyIfAbsent());
        assertEquals("Failed to get master key from config index", ex.getMessage());
    }

    public void testEncryptDecryptTemplateCredential() {
        encryptorUtils.setMasterKey(testMasterKey);

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
