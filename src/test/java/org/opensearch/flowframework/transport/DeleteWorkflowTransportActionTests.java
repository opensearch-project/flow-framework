/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteWorkflowTransportActionTests extends OpenSearchTestCase {

    private Client client;
    private DeleteWorkflowTransportAction deleteWorkflowTransportAction;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.client = mock(Client.class);
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        this.deleteWorkflowTransportAction = new DeleteWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            flowFrameworkIndicesHandler,
            client
        );

        ThreadPool clientThreadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);

    }

    public void testDeleteWorkflowNoGlobalContext() {

        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(false);
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest("1", null);
        deleteWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("There are no templates in the global context."));
    }

    public void testDeleteWorkflowSuccess() {
        String workflowId = "12345";
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(true);

        // Stub client.delete to force on response
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> responseListener = invocation.getArgument(1);

            ShardId shardId = new ShardId(new Index("indexName", "uuid"), 1);
            responseListener.onResponse(new DeleteResponse(shardId, workflowId, 1, 1, 1, true));
            return null;
        }).when(client).delete(any(DeleteRequest.class), any());

        deleteWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<DeleteResponse> responseCaptor = ArgumentCaptor.forClass(DeleteResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(Result.DELETED, responseCaptor.getValue().getResult());
    }

    public void testDeleteWorkflowNotFound() {
        String workflowId = "12345";
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(true);

        // Stub client.delete to force on response
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> responseListener = invocation.getArgument(1);

            ShardId shardId = new ShardId(new Index("indexName", "uuid"), 1);
            responseListener.onResponse(new DeleteResponse(shardId, workflowId, 1, 1, 1, false));
            return null;
        }).when(client).delete(any(DeleteRequest.class), any());

        deleteWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<DeleteResponse> responseCaptor = ArgumentCaptor.forClass(DeleteResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(Result.NOT_FOUND, responseCaptor.getValue().getResult());
    }
}
