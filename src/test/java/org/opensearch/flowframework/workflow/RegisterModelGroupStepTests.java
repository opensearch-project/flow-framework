/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ApiSpecFetcher;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.AccessMode;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.ML_COMMONS_API_SPEC_YAML_URI;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_STATUS;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_GROUP_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RegisterModelGroupStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;
    private WorkflowData inputDataWithNoName;
    private WorkflowData boolStringInputData;
    private WorkflowData badBoolInputData;

    private String modelGroupId = MODEL_GROUP_ID;
    private String status = MLTaskState.CREATED.name();

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);
        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("backend_roles", List.of("role-1")),
                Map.entry("access_mode", AccessMode.PUBLIC),
                Map.entry("add_all_backend_roles", false)
            ),
            "test-id",
            "test-node-id"
        );
        inputDataWithNoName = new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id");
        boolStringInputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("backend_roles", List.of("role-1")),
                Map.entry("access_mode", AccessMode.PUBLIC),
                Map.entry("add_all_backend_roles", "false")
            ),
            "test-id",
            "test-node-id"
        );
        badBoolInputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("backend_roles", List.of("role-1")),
                Map.entry("access_mode", AccessMode.PUBLIC),
                Map.entry("add_all_backend_roles", "no")
            ),
            "test-id",
            "test-node-id"
        );
    }

    public void testRegisterModelGroup() throws ExecutionException, InterruptedException, IOException {
        RegisterModelGroupStep modelGroupStep = new RegisterModelGroupStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLRegisterModelGroupResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelGroupResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelGroupResponse output = new MLRegisterModelGroupResponse(modelGroupId, status);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new WorkflowData(Map.of(MODEL_GROUP_ID, modelGroupId), "test-id", "test-node-id"));
            return null;
        }).when(flowFrameworkIndicesHandler).addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any());

        PlainActionFuture<WorkflowData> future = modelGroupStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        verify(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        assertEquals(modelGroupId, future.get().getContent().get(MODEL_GROUP_ID));
        assertEquals(status, future.get().getContent().get("model_group_status"));

        future = modelGroupStep.execute(
            boolStringInputData.getNodeId(),
            boolStringInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        assertTrue(future.isDone());
        assertEquals(modelGroupId, future.get().getContent().get(MODEL_GROUP_ID));
        assertEquals(status, future.get().getContent().get(MODEL_GROUP_STATUS));
    }

    public void testRegisterModelGroupFailure() throws IOException {
        RegisterModelGroupStep modelGroupStep = new RegisterModelGroupStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLRegisterModelGroupResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelGroupResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to register model group", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        PlainActionFuture<WorkflowData> future = modelGroupStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        verify(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register model group", ex.getCause().getMessage());

    }

    public void testRegisterModelGroupWithNoName() throws IOException {
        RegisterModelGroupStep modelGroupStep = new RegisterModelGroupStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        PlainActionFuture<WorkflowData> future = modelGroupStep.execute(
            inputDataWithNoName.getNodeId(),
            inputDataWithNoName,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Missing required inputs [name] in workflow [test-id] node [test-node-id]", ex.getCause().getMessage());
    }

    public void testBoolParseFail() throws IOException, ExecutionException, InterruptedException {
        RegisterModelGroupStep modelGroupStep = new RegisterModelGroupStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        PlainActionFuture<WorkflowData> future = modelGroupStep.execute(
            badBoolInputData.getNodeId(),
            badBoolInputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            "fakeTenantId"
        );

        assertTrue(future.isDone());
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(WorkflowStepException.class, e.getCause().getClass());
        WorkflowStepException w = (WorkflowStepException) e.getCause();
        assertEquals("Failed to parse value [no] as only [true] or [false] are allowed.", w.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, w.getRestStatus());
    }

    public void testApiSpecRegisterModelGroupInputParamComparison() throws Exception {
        List<String> requiredEnumParams = WorkflowStepFactory.WorkflowSteps.REGISTER_MODEL_GROUP.inputs();

        boolean isMatch = ApiSpecFetcher.compareRequiredFields(
            requiredEnumParams,
            ML_COMMONS_API_SPEC_YAML_URI,
            "/_plugins/_ml/model_groups/_register",
            RestRequest.Method.POST
        );

        assertTrue(isMatch);
    }
}
