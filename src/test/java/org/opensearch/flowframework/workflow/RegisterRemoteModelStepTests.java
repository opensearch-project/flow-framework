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
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RegisterRemoteModelStepTests extends OpenSearchTestCase {

    private RegisterRemoteModelStep registerRemoteModelStep;
    private WorkflowData workflowData;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Mock
    MachineLearningNodeClient mlNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);
        this.registerRemoteModelStep = new RegisterRemoteModelStep(mlNodeClient, flowFrameworkIndicesHandler);
        this.workflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("function_name", "remote"),
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry("connector_id", "abcdefg")
            ),
            "test-id",
            "test-node-id"
        );
    }

    public void testRegisterRemoteModelSuccess() throws Exception {

        String taskId = "abcd";
        String modelId = "efgh";
        String status = MLTaskState.CREATED.name();

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, status, modelId);
            actionListener.onResponse(output);
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        CompletableFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(mlNodeClient, times(1)).register(any(MLRegisterModelInput.class), any());

        assertTrue(future.isDone());
        assertTrue(!future.isCompletedExceptionally());
        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));

    }

    public void testRegisterRemoteModelFailure() {
        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IllegalArgumentException("test"));
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        CompletableFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("test", ex.getCause().getMessage());

    }

    public void testMissingInputs() {
        CompletableFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            "nodeId",
            WorkflowData.EMPTY,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Required fields are not provided", ex.getCause().getMessage());
    }

}
