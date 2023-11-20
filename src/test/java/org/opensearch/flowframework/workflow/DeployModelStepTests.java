/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.MLTaskType;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DeployModelStepTests extends OpenSearchTestCase {

    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        inputData = new WorkflowData(Map.ofEntries(Map.entry("model_id", "modelId")), "test-id");

        MockitoAnnotations.openMocks(this);

    }

    public void testDeployModel() throws ExecutionException, InterruptedException {
        String taskId = "taskId";
        String status = MLTaskState.CREATED.name();
        MLTaskType mlTaskType = MLTaskType.DEPLOY_MODEL;

        DeployModelStep deployModel = new DeployModelStep(machineLearningNodeClient);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLDeployModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLDeployModelResponse> actionListener = invocation.getArgument(1);
            MLDeployModelResponse output = new MLDeployModelResponse(taskId, mlTaskType, status);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = deployModel.execute(List.of(inputData));

        verify(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        assertEquals(status, future.get().getContent().get("deploy_model_status"));
    }

    public void testDeployModelFailure() {
        DeployModelStep deployModel = new DeployModelStep(machineLearningNodeClient);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLDeployModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLDeployModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to deploy model", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = deployModel.execute(List.of(inputData));

        verify(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to deploy model", ex.getCause().getMessage());
    }
}
