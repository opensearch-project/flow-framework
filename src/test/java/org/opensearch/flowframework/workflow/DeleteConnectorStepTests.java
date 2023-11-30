/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

public class DeleteConnectorStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);

        inputData = new WorkflowData(Map.of(CommonValue.CONNECTOR_ID, "test"), "test-id", "test-node-id");
    }

    public void testDeleteConnector() throws IOException, ExecutionException, InterruptedException {

        String connectorId = "connectorId";
        DeleteConnectorStep deleteConnectorStep = new DeleteConnectorStep(machineLearningNodeClient);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<DeleteResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> actionListener = invocation.getArgument(1);
            ShardId shardId = new ShardId(new Index("indexName", "uuid"), 1);
            DeleteResponse output = new DeleteResponse(shardId, connectorId, 1, 1, 1, true);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).deleteConnector(any(String.class), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = deleteConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of("connector_id", "test"), "workflowId", "nodeId")),
            Map.of("step_1", "connector_id")
        );
        verify(machineLearningNodeClient).deleteConnector(any(String.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        assertEquals(connectorId, future.get().getContent().get("connector_id"));

    }

    public void testNoConnectorIdInOutput() throws IOException {
        DeleteConnectorStep deleteConnectorStep = new DeleteConnectorStep(machineLearningNodeClient);

        CompletableFuture<WorkflowData> future = deleteConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Required field connector_id is not provided", ex.getCause().getMessage());
    }

    public void testDeleteConnectorFailure() throws IOException {
        DeleteConnectorStep deleteConnectorStep = new DeleteConnectorStep(machineLearningNodeClient);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<DeleteResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLCreateConnectorResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to delete connector", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).deleteConnector(any(String.class), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = deleteConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of("connector_id", "test"), "workflowId", "nodeId")),
            Map.of("step_1", "connector_id")
        );

        verify(machineLearningNodeClient).deleteConnector(any(String.class), actionListenerCaptor.capture());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to delete connector", ex.getCause().getMessage());
    }
}
