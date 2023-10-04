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

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.MLTaskType;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DeployModelTests extends OpenSearchTestCase {

    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private NodeClient nodeClient;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        inputData = new WorkflowData(Map.ofEntries(Map.entry("model_id", "modelId")));

        MockitoAnnotations.openMocks(this);

        nodeClient = new NoOpNodeClient("xyz");

    }

    public void testDeployModel() {

        String taskId = "taskId";
        String status = MLTaskState.CREATED.name();
        MLTaskType mlTaskType = MLTaskType.DEPLOY_MODEL;

        DeployModel deployModel = new DeployModel(nodeClient);

        ArgumentCaptor<ActionListener<MLDeployModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLDeployModelResponse> actionListener = invocation.getArgument(2);
            MLDeployModelResponse output = new MLDeployModelResponse(taskId, mlTaskType, status);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = deployModel.execute(List.of(inputData));

        // TODO: Find a way to verify the below
        // verify(machineLearningNodeClient).deploy(eq(MLRegisterModelInput.class), actionListenerCaptor.capture());

        assertTrue(future.isCompletedExceptionally());

    }
}
