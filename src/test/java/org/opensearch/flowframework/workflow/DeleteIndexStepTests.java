/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteIndexStepTests extends OpenSearchTestCase {

    private WorkflowData inputData = WorkflowData.EMPTY;
    private Client client;
    private AdminClient adminClient;
    private DeleteIndexStep deleteIndexStep;
    private ThreadContext threadContext;
    private Metadata metadata;

    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private ThreadPool threadPool;
    @Mock
    IndexMetadata indexMetadata;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        inputData = new WorkflowData(Map.ofEntries(Map.entry(INDEX_NAME, "demo")), "test-id", "test-node-id");
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

        deleteIndexStep = new DeleteIndexStep(client);
    }

    public void testDeleteIndex() throws IOException, ExecutionException, InterruptedException {

        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<AcknowledgedResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = deleteIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).delete(any(DeleteIndexRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new AcknowledgedResponse(true));

        assertTrue(future.isDone());

        Map<String, Object> outputData = Map.of(INDEX_NAME, "demo");
        assertEquals(outputData, future.get().getContent());
    }

    public void testDeleteIndexStepFailure() throws ExecutionException, InterruptedException {
        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<AcknowledgedResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = deleteIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).delete(any(DeleteIndexRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onFailure(new Exception("Failed to delete the index"));

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof Exception);
        assertEquals("Failed to delete the index demo", ex.getCause().getMessage());
    }
}
