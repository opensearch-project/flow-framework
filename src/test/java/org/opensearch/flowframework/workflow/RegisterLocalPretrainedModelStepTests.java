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

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.DEPLOY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RegisterLocalPretrainedModelStepTests extends OpenSearchTestCase {

    private static TestThreadPool testThreadPool;
    private RegisterLocalPretrainedModelStep registerLocalPretrainedModelStep;
    private WorkflowData workflowData;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private FlowFrameworkSettings flowFrameworkSettings;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);
        when(flowFrameworkSettings.getRetryDuration()).thenReturn(TimeValue.timeValueSeconds(5));

        testThreadPool = new TestThreadPool(
            RegisterLocalCustomModelStepTests.class.getName(),
            new ScalingExecutorBuilder(
                WORKFLOW_THREAD_POOL,
                1,
                Math.max(1, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
                TimeValue.timeValueMinutes(1),
                FLOW_FRAMEWORK_THREAD_POOL_PREFIX + WORKFLOW_THREAD_POOL
            ),
            new ScalingExecutorBuilder(
                PROVISION_WORKFLOW_THREAD_POOL,
                1,
                Math.max(1, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
                TimeValue.timeValueMinutes(5),
                FLOW_FRAMEWORK_THREAD_POOL_PREFIX + PROVISION_WORKFLOW_THREAD_POOL
            )
        );
        this.registerLocalPretrainedModelStep = new RegisterLocalPretrainedModelStep(
            testThreadPool,
            machineLearningNodeClient,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings
        );

        this.workflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("version", "1.0.0"),
                Map.entry("model_format", "TORCH_SCRIPT"),
                Map.entry(MODEL_GROUP_ID, "abcdefg"),
                Map.entry("description", "aiwoeifjoaijeofiwe")
            ),
            "test-id",
            "test-node-id"
        );

    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testRegisterLocalPretrainedModelSuccess() throws Exception {

        String taskId = "abcd";
        String modelId = "model-id";
        String status = MLTaskState.COMPLETED.name();

        // Stub register for success case
        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, status, null);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).register(any(MLRegisterModelInput.class), any());

        // Stub getTask for success case
        doAnswer(invocation -> {
            ActionListener<MLTask> actionListener = invocation.getArgument(1);
            MLTask output = MLTask.builder().taskId(taskId).modelId(modelId).state(MLTaskState.COMPLETED).async(false).build();
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).getTask(any(), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new WorkflowData(Map.of(MODEL_ID, modelId), "test-id", "test-node-id"));
            return null;
        }).when(flowFrameworkIndicesHandler).addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any());

        PlainActionFuture<WorkflowData> future = registerLocalPretrainedModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        future.actionGet();

        verify(machineLearningNodeClient, times(1)).register(any(MLRegisterModelInput.class), any());
        verify(machineLearningNodeClient, times(1)).getTask(any(), any());

        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));

        WorkflowData boolStringWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("version", "1.0.0"),
                Map.entry("model_format", "TORCH_SCRIPT"),
                Map.entry(MODEL_GROUP_ID, "abcdefg"),
                Map.entry("description", "aiwoeifjoaijeofiwe"),
                Map.entry(DEPLOY_FIELD, "false")
            ),
            "test-id",
            "test-node-id"
        );

        future = registerLocalPretrainedModelStep.execute(
            boolStringWorkflowData.getNodeId(),
            boolStringWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        future.actionGet();

        verify(machineLearningNodeClient, times(2)).register(any(MLRegisterModelInput.class), any());
        verify(machineLearningNodeClient, times(2)).getTask(any(), any());

        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));
    }

    public void testRegisterLocalPretrainedModelFailure() {

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IllegalArgumentException("Failed to register local model in step test-node-id"));
            return null;
        }).when(machineLearningNodeClient).register(any(MLRegisterModelInput.class), any());

        PlainActionFuture<WorkflowData> future = this.registerLocalPretrainedModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register local model in step test-node-id", ex.getCause().getMessage());
    }

    public void testRegisterLocalPretrainedModelTaskFailure() {

        String taskId = "abcd";
        String modelId = "model-id";
        String testErrorMessage = "error";

        // Stub register for success case
        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, MLTaskState.RUNNING.name(), null);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).register(any(MLRegisterModelInput.class), any());

        // Stub get ml task for failure case
        doAnswer(invocation -> {
            ActionListener<MLTask> actionListener = invocation.getArgument(1);
            MLTask output = MLTask.builder().taskId(taskId).modelId(modelId).state(MLTaskState.COMPLETED).async(false).build();
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).getTask(any(), any());

        PlainActionFuture<WorkflowData> future = this.registerLocalPretrainedModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Local model registration failed with error : " + testErrorMessage, ex.getCause().getMessage());
    }

    public void testMissingInputs() {
        PlainActionFuture<WorkflowData> future = registerLocalPretrainedModelStep.execute(
            "nodeId",
            new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id"),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertTrue(ex.getCause().getMessage().startsWith("Missing required inputs ["));
        for (String s : new String[] { "model_format", "name", "version" }) {
            assertTrue(ex.getCause().getMessage().contains(s));
        }
        assertTrue(ex.getCause().getMessage().endsWith("] in workflow [test-id] node [test-node-id]"));
    }

    public void testBoolParseFail() throws IOException, ExecutionException, InterruptedException {
        WorkflowData boolStringWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("version", "1.0.0"),
                Map.entry("model_format", "TORCH_SCRIPT"),
                Map.entry(MODEL_GROUP_ID, "abcdefg"),
                Map.entry("description", "aiwoeifjoaijeofiwe"),
                Map.entry(DEPLOY_FIELD, "no")
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = registerLocalPretrainedModelStep.execute(
            boolStringWorkflowData.getNodeId(),
            boolStringWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get());
        assertEquals(WorkflowStepException.class, e.getCause().getClass());
        WorkflowStepException w = (WorkflowStepException) e.getCause();
        assertEquals("Failed to parse value [no] as only [true] or [false] are allowed.", w.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, w.getRestStatus());
    }
}
