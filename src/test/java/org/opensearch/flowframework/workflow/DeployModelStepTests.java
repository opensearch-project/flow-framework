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
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.MLTaskType;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;
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

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DeployModelStepTests extends OpenSearchTestCase {

    private static TestThreadPool testThreadPool;
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    private DeployModelStep deployModel;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private FlowFrameworkSettings flowFrameworkSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);
        when(flowFrameworkSettings.getRetryDuration()).thenReturn(TimeValue.timeValueSeconds(5));

        testThreadPool = new TestThreadPool(
            DeployModelStepTests.class.getName(),
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
        this.deployModel = new DeployModelStep(
            testThreadPool,
            machineLearningNodeClient,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings
        );
        this.inputData = new WorkflowData(Map.ofEntries(Map.entry("model_id", "modelId")), "test-id", "test-node-id");
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testDeployModel() throws ExecutionException, InterruptedException, IOException {
        String modelId = "modelId";
        String taskId = "taskId";

        String status = MLTaskState.COMPLETED.name();
        MLTaskType mlTaskType = MLTaskType.DEPLOY_MODEL;

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLDeployModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLDeployModelResponse> actionListener = invocation.getArgument(1);
            MLDeployModelResponse output = new MLDeployModelResponse(taskId, mlTaskType, status);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).deploy(eq(modelId), actionListenerCaptor.capture());

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

        PlainActionFuture<WorkflowData> future = deployModel.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        future.actionGet();

        verify(machineLearningNodeClient, times(1)).deploy(any(String.class), any());
        verify(machineLearningNodeClient, times(1)).getTask(any(), any());

        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
    }

    public void testDeployModelFailure() {

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLDeployModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLDeployModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to deploy model", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        PlainActionFuture<WorkflowData> future = deployModel.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        verify(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to deploy model modelId", ex.getCause().getMessage());
    }

    public void testDeployModelTaskFailure() throws IOException, InterruptedException, ExecutionException {
        String modelId = "modelId";
        String taskId = "taskId";

        String status = MLTaskState.RUNNING.name();
        MLTaskType mlTaskType = MLTaskType.DEPLOY_MODEL;

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLDeployModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLDeployModelResponse> actionListener = invocation.getArgument(1);
            MLDeployModelResponse output = new MLDeployModelResponse(taskId, mlTaskType, status);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).deploy(eq(modelId), actionListenerCaptor.capture());

        // Stub getTask for success case
        doAnswer(invocation -> {
            ActionListener<MLTask> actionListener = invocation.getArgument(1);
            MLTask output = MLTask.builder().taskId(taskId).modelId(modelId).state(MLTaskState.FAILED).async(false).error("error").build();
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).getTask(any(), any());

        PlainActionFuture<WorkflowData> future = this.deployModel.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to deploy model", ex.getCause().getMessage());
    }
}
