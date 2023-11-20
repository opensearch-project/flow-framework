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

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.common.FlowFrameworkMaxRequestRetrySetting;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.TASK_ID;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_REQUEST_RETRY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class GetMLTaskStepTests extends OpenSearchTestCase {

    private GetMLTaskStep getMLTaskStep;
    private WorkflowData workflowData;
    private FlowFrameworkMaxRequestRetrySetting maxRequestRetrySetting;

    @Mock
    MachineLearningNodeClient mlNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);
        ClusterService clusterService = mock(ClusterService.class);
        final Set<Setting<?>> settingsSet = Stream.concat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(), Stream.of(MAX_REQUEST_RETRY))
            .collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        this.maxRequestRetrySetting = new FlowFrameworkMaxRequestRetrySetting(clusterService, Settings.EMPTY);

        this.getMLTaskStep = new GetMLTaskStep(mlNodeClient, maxRequestRetrySetting);
        this.workflowData = new WorkflowData(Map.ofEntries(Map.entry(TASK_ID, "test")), "test-id");
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
