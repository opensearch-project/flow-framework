/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import com.google.common.collect.ImmutableList;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.AccessMode;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

public class ModelGroupStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);
        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("backend_roles", ImmutableList.of("role-1")),
                Map.entry("access_mode", AccessMode.PUBLIC),
                Map.entry("add_all_backend_roles", false)
            )
        );
    }

    public void testRegisterModelGroup() throws ExecutionException, InterruptedException, IOException {
        String modelGroupId = "model_group_id";
        String status = MLTaskState.CREATED.name();

        ModelGroupStep modelGroupStep = new ModelGroupStep(machineLearningNodeClient);

        ArgumentCaptor<ActionListener<MLRegisterModelGroupResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelGroupResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelGroupResponse output = new MLRegisterModelGroupResponse(modelGroupId, status);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = modelGroupStep.execute(List.of(inputData));

        verify(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        assertEquals(modelGroupId, future.get().getContent().get("model_group_id"));
        assertEquals(status, future.get().getContent().get("model_group_status"));

    }

    public void testRegisterModelGroupFailure() throws ExecutionException, InterruptedException, IOException {
        ModelGroupStep modelGroupStep = new ModelGroupStep(machineLearningNodeClient);

        ArgumentCaptor<ActionListener<MLRegisterModelGroupResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelGroupResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to register model group", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = modelGroupStep.execute(List.of(inputData));

        verify(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register model group", ex.getCause().getMessage());

    }

}
