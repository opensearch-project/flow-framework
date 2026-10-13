/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.OpenSearchException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.transport.undeploy.MLUndeployModelNodesResponse;
import org.opensearch.ml.common.transport.undeploy.MLUndeployModelsResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.SUCCESS;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

public class UndeployModelStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);

        inputData = new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id");
    }

    public void testUndeployModel() throws IOException, ExecutionException, InterruptedException {

        String modelId = randomAlphaOfLength(5);
        UndeployModelStep UndeployModelStep = new UndeployModelStep(machineLearningNodeClient);

        doAnswer(invocation -> {
            ClusterName clusterName = new ClusterName("clusterName");
            ActionListener<MLUndeployModelsResponse> actionListener = invocation.getArgument(3);
            MLUndeployModelNodesResponse mlUndeployModelNodesResponse = new MLUndeployModelNodesResponse(
                clusterName,
                Collections.emptyList(),
                Collections.emptyList()
            );
            MLUndeployModelsResponse output = new MLUndeployModelsResponse(mlUndeployModelNodesResponse);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).undeploy(any(String[].class), any(), nullable(String.class), any());

        PlainActionFuture<WorkflowData> future = UndeployModelStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of(MODEL_ID, modelId), "workflowId", "nodeId")),
            Map.of("step_1", MODEL_ID),
            Collections.emptyMap(),
            null
        );
        verify(machineLearningNodeClient).undeploy(any(String[].class), any(), nullable(String.class), any());

        assertTrue(future.isDone());
        assertTrue((boolean) future.get().getContent().get(SUCCESS));
    }

    public void testNoModelIdInOutput() throws IOException {
        UndeployModelStep UndeployModelStep = new UndeployModelStep(machineLearningNodeClient);

        PlainActionFuture<WorkflowData> future = UndeployModelStep.execute(
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
        assertEquals("Missing required inputs [model_id] in workflow [test-id] node [test-node-id]", ex.getCause().getMessage());
    }

    public void testUndeployModelFailure() throws IOException {
        UndeployModelStep UndeployModelStep = new UndeployModelStep(machineLearningNodeClient);

        doAnswer(invocation -> {
            ClusterName clusterName = new ClusterName("clusterName");
            ActionListener<MLUndeployModelsResponse> actionListener = invocation.getArgument(3);
            MLUndeployModelNodesResponse mlUndeployModelNodesResponse = new MLUndeployModelNodesResponse(
                clusterName,
                Collections.emptyList(),
                List.of(new FailedNodeException("failed-node", "Test message", null))
            );
            MLUndeployModelsResponse output = new MLUndeployModelsResponse(mlUndeployModelNodesResponse);
            actionListener.onResponse(output);

            actionListener.onFailure(new FlowFrameworkException("Failed to undeploy model", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).undeploy(any(String[].class), any(), nullable(String.class), any());

        PlainActionFuture<WorkflowData> future = UndeployModelStep.execute(
            inputData.getNodeId(),
            inputData,
            Map.of("step_1", new WorkflowData(Map.of(MODEL_ID, "test"), "workflowId", "nodeId")),
            Map.of("step_1", MODEL_ID),
            Collections.emptyMap(),
            null
        );

        verify(machineLearningNodeClient).undeploy(any(String[].class), any(), nullable(String.class), any());

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof OpenSearchException);
        assertEquals("Failed to undeploy model on nodes [failed-node]", ex.getCause().getMessage());
    }
}
