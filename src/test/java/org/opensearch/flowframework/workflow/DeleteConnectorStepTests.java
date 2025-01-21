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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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

        inputData = new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id");
    }

    public void testDeleteConnector() throws IOException, ExecutionException, InterruptedException {

        String connectorId = randomAlphaOfLength(5);
        DeleteConnectorStep deleteConnectorStep = new DeleteConnectorStep(machineLearningNodeClient);

        doAnswer(invocation -> {
            String connectorIdArg = invocation.getArgument(0);
            ActionListener<DeleteResponse> actionListener = invocation.getArgument(1);
            ShardId shardId = new ShardId(new Index("indexName", "uuid"), 1);
            DeleteResponse output = new DeleteResponse(shardId, connectorIdArg, 1, 1, 1, true);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).deleteConnector(anyString(), anyActionListener());

        PlainActionFuture<WorkflowData> future = deleteConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of(CONNECTOR_ID, connectorId), "workflowId", "nodeId")),
            Map.of("step_1", CONNECTOR_ID),
            Collections.emptyMap(),
            "fakeTenantId"
        );
        verify(machineLearningNodeClient).deleteConnector(anyString(), anyActionListener());

        assertTrue(future.isDone());
        assertEquals(connectorId, future.get().getContent().get(CONNECTOR_ID));
    }

    public void testNoConnectorIdInOutput() throws IOException {
        DeleteConnectorStep deleteConnectorStep = new DeleteConnectorStep(machineLearningNodeClient);

        PlainActionFuture<WorkflowData> future = deleteConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Missing required inputs [connector_id] in workflow [test-id] node [test-node-id]", ex.getCause().getMessage());
    }

    public void testDeleteConnectorFailure() throws IOException {
        DeleteConnectorStep deleteConnectorStep = new DeleteConnectorStep(machineLearningNodeClient);

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to delete connector", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).deleteConnector(anyString(), anyActionListener());

        PlainActionFuture<WorkflowData> future = deleteConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of(CONNECTOR_ID, "test"), "workflowId", "nodeId")),
            Map.of("step_1", CONNECTOR_ID),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        verify(machineLearningNodeClient).deleteConnector(anyString(), anyActionListener());

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to delete connector test", ex.getCause().getMessage());
    }

    @SuppressWarnings("unchecked")
    private ActionListener<DeleteResponse> anyActionListener() {
        return any(ActionListener.class);
    }
}
