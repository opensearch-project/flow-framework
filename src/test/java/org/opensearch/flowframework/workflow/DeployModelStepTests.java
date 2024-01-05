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

import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
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
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_GET_TASK_REQUEST_RETRY;
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

        ClusterService clusterService = mock(ClusterService.class);
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(MAX_GET_TASK_REQUEST_RETRY)
        ).collect(Collectors.toSet());

        // Set max request retry setting to 1 to limit sleeping the thread to one retry iteration
        Settings testMaxRetrySetting = Settings.builder().put(MAX_GET_TASK_REQUEST_RETRY.getKey(), 1).build();
        ClusterSettings clusterSettings = new ClusterSettings(testMaxRetrySetting, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);
        when(flowFrameworkSettings.getMaxRetry()).thenReturn(5);

        testThreadPool = new TestThreadPool(
            DeployModelStepTests.class.getName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                WORKFLOW_THREAD_POOL,
                OpenSearchExecutors.allocatedProcessors(Settings.EMPTY),
                100,
                FLOW_FRAMEWORK_THREAD_POOL_PREFIX + WORKFLOW_THREAD_POOL
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
            MLTask output = new MLTask(
                taskId,
                modelId,
                null,
                null,
                MLTaskState.COMPLETED,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                false
            );
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).getTask(any(), any());

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateResourceInStateIndex(anyString(), anyString(), anyString(), anyString(), any());

        CompletableFuture<WorkflowData> future = deployModel.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        future.join();

        verify(machineLearningNodeClient, times(1)).deploy(any(String.class), any());
        verify(machineLearningNodeClient, times(1)).getTask(any(), any());

        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));
    }

    public void testDeployModelFailure() {

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLDeployModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLDeployModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to deploy model", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = deployModel.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(machineLearningNodeClient).deploy(eq("modelId"), actionListenerCaptor.capture());

        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to deploy model", ex.getCause().getMessage());
    }

    public void testDeployModelTaskFailure() throws IOException, InterruptedException, ExecutionException {
        String modelId = "modelId";
        String taskId = "taskId";

        String status = MLTaskState.RUNNING.name();
        MLTaskType mlTaskType = MLTaskType.DEPLOY_MODEL;
        String testErrorMessage = "error";

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
            MLTask output = new MLTask(
                taskId,
                modelId,
                null,
                null,
                MLTaskState.FAILED,
                null,
                null,
                null,
                null,
                null,
                null,
                testErrorMessage,
                null,
                false
            );
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).getTask(any(), any());

        CompletableFuture<WorkflowData> future = this.deployModel.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Deploy model failed with error : " + testErrorMessage, ex.getCause().getMessage());
    }
}
