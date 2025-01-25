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
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteIndexStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;

    @Mock
    private Client client;
    @Mock
    private AdminClient adminClient;
    @Mock
    private IndicesAdminClient indicesAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        inputData = new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id");
    }

    public void testDeleteIndex() throws IOException, ExecutionException, InterruptedException {

        String indexName = randomAlphaOfLength(5);
        DeleteIndexStep deleteIndexStep = new DeleteIndexStep(client);

        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(indicesAdminClient).delete(any(DeleteIndexRequest.class), any());

        PlainActionFuture<WorkflowData> future = deleteIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of(INDEX_NAME, indexName), "workflowId", "nodeId")),
            Map.of("step_1", INDEX_NAME),
            Collections.emptyMap(),
            null
        );
        verify(indicesAdminClient).delete(any(DeleteIndexRequest.class), any());

        assertTrue(future.isDone());
        assertEquals(indexName, future.get().getContent().get(INDEX_NAME));
    }

    public void testNoIndexNameInOutput() throws IOException {
        DeleteIndexStep deleteIndexStep = new DeleteIndexStep(client);

        PlainActionFuture<WorkflowData> future = deleteIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Missing required inputs [index_name] in workflow [test-id] node [test-node-id]", ex.getCause().getMessage());
    }

    public void testDeleteIndexFailure() throws IOException {
        DeleteIndexStep deleteIndexStep = new DeleteIndexStep(client);

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(indicesAdminClient).delete(any(DeleteIndexRequest.class), any());

        PlainActionFuture<WorkflowData> future = deleteIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of(INDEX_NAME, "test"), "workflowId", "nodeId")),
            Map.of("step_1", INDEX_NAME),
            Collections.emptyMap(),
            null
        );

        verify(indicesAdminClient).delete(any(DeleteIndexRequest.class), any());

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to delete the index test", ex.getCause().getMessage());
    }
}
