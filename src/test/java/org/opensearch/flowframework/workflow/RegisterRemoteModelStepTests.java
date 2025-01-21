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

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ApiSpecFetcher;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteTransportException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.DEPLOY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.INTERFACE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.ML_COMMONS_API_SPEC_YAML_URI;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
                Map.entry("function_name", "ignored"),
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry(CONNECTOR_ID, "abcdefg"),
                Map.entry(
                    INTERFACE_FIELD,
                    "{\"output\":{\"properties\":{\"inference_results\":{\"description\":\"This is a test description field\",\"type\":\"array\",\"items\":{\"type\":\"object\",\"properties\":{\"output\":{\"description\":\"This is a test description field\",\"type\":\"array\",\"items\":{\"properties\":{\"name\":{\"description\":\"This is a test description field\",\"type\":\"string\"},\"dataAsMap\":{\"description\":\"This is a test description field\",\"type\":\"object\"}}}},\"status_code\":{\"description\":\"This is a test description field\",\"type\":\"integer\"}}}}}},\"input\":{\"properties\":{\"parameters\":{\"properties\":{\"messages\":{\"description\":\"This is a test description field\",\"type\":\"string\"}}}}}}"
                )
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

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new WorkflowData(Map.of(MODEL_ID, modelId), "test-id", "test-node-id"));
            return null;
        }).when(flowFrameworkIndicesHandler).addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any());

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        verify(mlNodeClient, times(1)).register(any(MLRegisterModelInput.class), any());
        // only updates register resource
        verify(flowFrameworkIndicesHandler, times(1)).addResourceToStateIndex(
            any(WorkflowData.class),
            anyString(),
            anyString(),
            anyString(),
            any()
        );

        assertTrue(future.isDone());
        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));
    }

    public void testRegisterAndDeployRemoteModelSuccess() throws Exception {

        String taskId = "abcd";
        String modelId = "efgh";
        String status = MLTaskState.CREATED.name();

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, status, modelId);
            actionListener.onResponse(output);
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new WorkflowData(Map.of(MODEL_ID, modelId), "test-id", "test-node-id"));
            return null;
        }).when(flowFrameworkIndicesHandler).addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any());

        WorkflowData deployWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry(CONNECTOR_ID, "abcdefg"),
                Map.entry(DEPLOY_FIELD, true)
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            deployWorkflowData.getNodeId(),
            deployWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        verify(mlNodeClient, times(1)).register(any(MLRegisterModelInput.class), any());
        // updates both register and deploy resources
        verify(flowFrameworkIndicesHandler, times(2)).addResourceToStateIndex(
            any(WorkflowData.class),
            anyString(),
            anyString(),
            anyString(),
            any()
        );

        assertTrue(future.isDone());
        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));

        deployWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry(CONNECTOR_ID, "abcdefg"),
                Map.entry(DEPLOY_FIELD, "true")
            ),
            "test-id",
            "test-node-id"
        );
        future = this.registerRemoteModelStep.execute(
            deployWorkflowData.getNodeId(),
            deployWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        verify(mlNodeClient, times(2)).register(any(MLRegisterModelInput.class), any());
        // updates both register and deploy resources
        verify(flowFrameworkIndicesHandler, times(4)).addResourceToStateIndex(
            any(WorkflowData.class),
            anyString(),
            anyString(),
            anyString(),
            any()
        );

        assertTrue(future.isDone());
        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));
    }

    public void testRegisterRemoteModelFailure() {
        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IllegalArgumentException("Failed to register remote model"));
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );
        assertTrue(future.isDone());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register remote model", ex.getCause().getMessage());

    }

    public void testRegisterRemoteModelUpdateFailure() {
        String taskId = "abcd";
        String modelId = "efgh";
        String status = MLTaskState.CREATED.name();

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, status, modelId);
            actionListener.onResponse(output);
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onFailure(new RuntimeException("Failed to update register resource"));
            return null;
        }).when(flowFrameworkIndicesHandler).addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any());

        WorkflowData deployWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry(CONNECTOR_ID, "abcdefg"),
                Map.entry(DEPLOY_FIELD, true)
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            deployWorkflowData.getNodeId(),
            deployWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        assertTrue(future.isDone());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to update new created test-node-id resource register_remote_model id efgh", ex.getCause().getMessage());
    }

    public void testRegisterRemoteModelDeployUpdateFailure() {
        String taskId = "abcd";
        String modelId = "efgh";
        String status = MLTaskState.CREATED.name();

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, status, modelId);
            actionListener.onResponse(output);
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(4);
            if (invocationCount.getAndIncrement() == 0) {
                // succeed on first call (update register)
                updateResponseListener.onResponse(new WorkflowData(Map.of(MODEL_ID, modelId), "test-id", "test-node-id"));
            } else {
                // fail on second call (update deploy)
                updateResponseListener.onFailure(new RuntimeException("Failed to update deploy resource"));
            }
            return null;
        }).when(flowFrameworkIndicesHandler).addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any());

        WorkflowData deployWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry(CONNECTOR_ID, "abcdefg"),
                Map.entry(DEPLOY_FIELD, true)
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            deployWorkflowData.getNodeId(),
            deployWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        assertTrue(future.isDone());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to update simulated deploy step resource efgh", ex.getCause().getMessage());
    }

    public void testReisterRemoteModelInterfaceFailure() {
        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IllegalArgumentException("Failed to register remote model"));
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        WorkflowData incorrectWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("function_name", "ignored"),
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry(CONNECTOR_ID, "abcdefg"),
                Map.entry(INTERFACE_FIELD, "{\"output\":")
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            incorrectWorkflowData.getNodeId(),
            incorrectWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );
        assertTrue(future.isDone());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to create model interface", ex.getCause().getMessage());
    }

    public void testRegisterRemoteModelUnSafeFailure() {
        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RemoteTransportException("test", new ResourceNotFoundException("test")));
            return null;
        }).when(mlNodeClient).register(any(MLRegisterModelInput.class), any());

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );
        assertTrue(future.isDone());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register remote model", ex.getCause().getMessage());

    }

    public void testMissingInputs() {
        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            "nodeId",
            new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id"),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );
        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertTrue(ex.getCause().getMessage().startsWith("Missing required inputs ["));
        for (String s : new String[] { "name", CONNECTOR_ID }) {
            assertTrue(ex.getCause().getMessage().contains(s));
        }
        assertTrue(ex.getCause().getMessage().endsWith("] in workflow [test-id] node [test-node-id]"));
    }

    public void testBoolParseFail() throws IOException, ExecutionException, InterruptedException {
        WorkflowData deployWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry(CONNECTOR_ID, "abcdefg"),
                Map.entry(DEPLOY_FIELD, "yes")
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = this.registerRemoteModelStep.execute(
            deployWorkflowData.getNodeId(),
            deployWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        assertTrue(future.isDone());
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(WorkflowStepException.class, e.getCause().getClass());
        WorkflowStepException w = (WorkflowStepException) e.getCause();
        assertEquals("Failed to parse value [yes] as only [true] or [false] are allowed.", w.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, w.getRestStatus());
    }

    public void testApiSpecRegisterRemoteModelInputParamComparison() throws Exception {
        List<String> requiredEnumParams = WorkflowStepFactory.WorkflowSteps.REGISTER_REMOTE_MODEL.inputs();

        boolean isMatch = ApiSpecFetcher.compareRequiredFields(
            requiredEnumParams,
            ML_COMMONS_API_SPEC_YAML_URI,
            "/_plugins/_ml/model_groups/_register",
            RestRequest.Method.POST
        );

        assertTrue(isMatch);
    }
}
