/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteTransportException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateIndexStepTests extends OpenSearchTestCase {

    private WorkflowData inputData = WorkflowData.EMPTY;
    private Client client;
    private AdminClient adminClient;
    private CreateIndexStep createIndexStep;
    private ThreadContext threadContext;
    private Metadata metadata;

    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private ThreadPool threadPool;
    @Mock
    IndexMetadata indexMetadata;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);
        String configurations =
            "{\"settings\":{\"index\":{\"number_of_shards\":2,\"number_of_replicas\":1}},\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}},\"aliases\":{\"sample-alias1\":{}}}";
        inputData = new WorkflowData(
            Map.ofEntries(Map.entry(INDEX_NAME, "demo"), Map.entry(CONFIGURATIONS, configurations)),
            "test-id",
            "test-node-id"
        );
        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        metadata = mock(Metadata.class);
        Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);

        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(metadata.indices()).thenReturn(Map.of(GLOBAL_CONTEXT_INDEX, indexMetadata));

        createIndexStep = new CreateIndexStep(client, flowFrameworkIndicesHandler);
    }

    public void testCreateIndexStep() throws ExecutionException, InterruptedException, IOException {

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new WorkflowData(Map.of(INDEX_NAME, "demo"), "test-id", "test-node-id"));
            return null;
        }).when(flowFrameworkIndicesHandler).addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any());

        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<CreateIndexResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = createIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new CreateIndexResponse(true, true, "demo"));

        assertTrue(future.isDone());

        Map<String, Object> outputData = Map.of(INDEX_NAME, "demo");
        assertEquals(outputData, future.get().getContent());

    }

    public void testCreateIndexStepFailure() throws ExecutionException, InterruptedException {
        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<CreateIndexResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = createIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), actionListenerCaptor.capture());

        actionListenerCaptor.getValue().onFailure(new Exception("Failed to create an index"));

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof Exception);
        assertEquals("Failed to create the index demo", ex.getCause().getMessage());
    }

    public void testCreateIndexStepUnsafeFailure() throws ExecutionException, InterruptedException, IOException {
        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<CreateIndexResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = createIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), actionListenerCaptor.capture());

        actionListenerCaptor.getValue().onFailure(new RemoteTransportException("test", new ResourceNotFoundException("test")));

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof Exception);
        assertEquals("Failed to create the index demo", ex.getCause().getMessage());
    }
}
