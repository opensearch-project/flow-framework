/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import com.google.common.collect.ImmutableMap;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import static org.opensearch.flowframework.common.CommonValue.MASTER_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EncryptorUtilsTests extends OpenSearchTestCase {

    private ClusterService clusterService;
    private Client client;
    private EncryptorUtils encryptorUtils;
    private String testMasterKey;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.clusterService = mock(ClusterService.class);
        this.client = mock(Client.class);
        this.encryptorUtils = new EncryptorUtils(clusterService, client);
        this.testMasterKey = encryptorUtils.generateMasterKey();

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

    public void testInitializeMasterKeySuccess() {
        encryptorUtils.setMasterKey(null);

        String masterKey = encryptorUtils.generateMasterKey();
        doAnswer(invocation -> {
            ActionListener<GetResponse> getRequestActionListener = invocation.getArgument(1);

            // Stub get response for success case
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(true);
            when(getResponse.getSourceAsMap()).thenReturn(ImmutableMap.of(MASTER_KEY, masterKey));

            getRequestActionListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        encryptorUtils.initializeMasterKey();
        assertEquals(masterKey, encryptorUtils.getMasterKey());
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

        FlowFrameworkException ex = expectThrows(FlowFrameworkException.class, () -> encryptorUtils.initializeMasterKey());
        assertEquals("Encryption key has not been initialized", ex.getMessage());
    }

    // TODO : test encrypting test template
    // TODO : test decrypting test template

}
