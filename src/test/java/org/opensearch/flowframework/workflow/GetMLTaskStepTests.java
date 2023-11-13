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
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.TASK_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class GetMLTaskStepTests extends OpenSearchTestCase {

    private GetMLTaskStep getMLTaskStep;
    private WorkflowData workflowData;

    @Mock
    MachineLearningNodeClient mlNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);
        this.getMLTaskStep = new GetMLTaskStep(mlNodeClient);
        this.workflowData = new WorkflowData(Map.ofEntries(Map.entry(TASK_ID, "test")));
    }

    public void testGetMLTaskSuccess() throws Exception {
        String taskId = "test";
        String modelId = "abcd";
        MLTaskState status = MLTaskState.COMPLETED;

        doAnswer(invocation -> {
            ActionListener<MLTask> actionListener = invocation.getArgument(1);
            MLTask output = new MLTask(taskId, modelId, null, null, status, null, null, null, null, null, null, null, null, false);
            actionListener.onResponse(output);
            return null;
        }).when(mlNodeClient).getTask(any(), any());

        CompletableFuture<WorkflowData> future = this.getMLTaskStep.execute(List.of(workflowData));

        verify(mlNodeClient, times(1)).getTask(any(), any());

        assertTrue(future.isDone());
        assertTrue(!future.isCompletedExceptionally());
        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status.name(), future.get().getContent().get(REGISTER_MODEL_STATUS));
    }

    public void testGetMLTaskFailure() {
        doAnswer(invocation -> {
            ActionListener<MLTask> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IllegalArgumentException("test"));
            return null;
        }).when(mlNodeClient).getTask(any(), any());

        CompletableFuture<WorkflowData> future = this.getMLTaskStep.execute(List.of(workflowData));
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("test", ex.getCause().getMessage());
    }

    public void testMissingInputs() {
        CompletableFuture<WorkflowData> future = this.getMLTaskStep.execute(List.of(WorkflowData.EMPTY));
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Required fields are not provided", ex.getCause().getMessage());
    }

}
