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
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

public class DeleteModelStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);

        inputData = new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id");
    }

    public void testDeleteModel() throws IOException, ExecutionException, InterruptedException {

        String modelId = randomAlphaOfLength(5);
        DeleteModelStep deleteModelStep = new DeleteModelStep(machineLearningNodeClient);

        doAnswer(invocation -> {
            String modelIdArg = invocation.getArgument(0);
            ActionListener<DeleteResponse> actionListener = invocation.getArgument(1);
            ShardId shardId = new ShardId(new Index("indexName", "uuid"), 1);
            DeleteResponse output = new DeleteResponse(shardId, modelIdArg, 1, 1, 1, true);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).deleteModel(any(String.class), any());

        CompletableFuture<WorkflowData> future = deleteModelStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of("model_id", modelId), "workflowId", "nodeId")),
            Map.of("step_1", "model_id")
        );
        verify(machineLearningNodeClient).deleteModel(any(String.class), any());

        assertTrue(future.isDone());
        assertEquals(modelId, future.get().getContent().get("model_id"));
    }

    public void testNoModelIdInOutput() throws IOException {
        DeleteModelStep deleteModelStep = new DeleteModelStep(machineLearningNodeClient);

        CompletableFuture<WorkflowData> future = deleteModelStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Missing required inputs [model_id] in workflow [test-id] node [test-node-id]", ex.getCause().getMessage());
    }

    public void testDeleteModelFailure() throws IOException {
        DeleteModelStep deleteModelStep = new DeleteModelStep(machineLearningNodeClient);

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to delete model", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).deleteModel(any(String.class), any());

        CompletableFuture<WorkflowData> future = deleteModelStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of("model_id", "test"), "workflowId", "nodeId")),
            Map.of("step_1", "model_id")
        );

        verify(machineLearningNodeClient).deleteModel(any(String.class), any());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to delete model", ex.getCause().getMessage());
    }
}
