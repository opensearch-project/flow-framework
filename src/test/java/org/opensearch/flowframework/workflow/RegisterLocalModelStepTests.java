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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_GET_TASK_REQUEST_RETRY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RegisterLocalModelStepTests extends OpenSearchTestCase {

    private RegisterLocalModelStep registerLocalModelStep;
    private WorkflowData workflowData;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

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

        // Set max request retry setting to 0 to avoid sleeping the thread during unit test failure cases
        Settings testMaxRetrySetting = Settings.builder().put(MAX_GET_TASK_REQUEST_RETRY.getKey(), 0).build();
        ClusterSettings clusterSettings = new ClusterSettings(testMaxRetrySetting, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        this.registerLocalModelStep = new RegisterLocalModelStep(
            testMaxRetrySetting,
            clusterService,
            machineLearningNodeClient,
            flowFrameworkIndicesHandler
        );

        this.workflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("version", "1.0.0"),
                Map.entry("description", "description"),
                Map.entry("model_format", "TORCH_SCRIPT"),
                Map.entry("model_group_id", "abcdefg"),
                Map.entry("model_content_hash_value", "aiwoeifjoaijeofiwe"),
                Map.entry("model_type", "bert"),
                Map.entry("embedding_dimension", "384"),
                Map.entry("framework_type", "sentence_transformers"),
                Map.entry("url", "something.com")
            ),
            "test-id",
            "test-node-id"
        );

    }

    public void testRegisterLocalModelSuccess() throws Exception {

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

        CompletableFuture<WorkflowData> future = registerLocalModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        ;
        verify(machineLearningNodeClient, times(1)).register(any(MLRegisterModelInput.class), any());
        verify(machineLearningNodeClient, times(1)).getTask(any(), any());

        assertTrue(future.isDone());
        assertTrue(!future.isCompletedExceptionally());
        assertEquals(modelId, future.get().getContent().get(MODEL_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));

    }

    public void testRegisterLocalModelFailure() {

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IllegalArgumentException("test"));
            return null;
        }).when(machineLearningNodeClient).register(any(MLRegisterModelInput.class), any());

        CompletableFuture<WorkflowData> future = this.registerLocalModelStep.execute(
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

    public void testRegisterLocalModelTaskFailure() {

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

        CompletableFuture<WorkflowData> future = this.registerLocalModelStep.execute(
            workflowData.getNodeId(),
            workflowData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Local model registration failed with error : " + testErrorMessage, ex.getCause().getMessage());

    }

    public void testMissingInputs() {
        CompletableFuture<WorkflowData> future = registerLocalModelStep.execute(
            "nodeId",
            new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id"),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertTrue(ex.getCause().getMessage().startsWith("Missing required inputs ["));
        for (String s : new String[] {
            "model_format",
            "name",
            "model_type",
            "embedding_dimension",
            "framework_type",
            "model_group_id",
            "version",
            "url",
            "model_content_hash_value" }) {
            assertTrue(ex.getCause().getMessage().contains(s));
        }
        assertTrue(ex.getCause().getMessage().endsWith("] in workflow [test-id] node [test-node-id]"));
    }
}
