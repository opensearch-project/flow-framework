/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.workflow.DeleteConnectorStep;
import org.opensearch.flowframework.workflow.DeleteIndexStep;
import org.opensearch.flowframework.workflow.DeleteIngestPipelineStep;
import org.opensearch.flowframework.workflow.UndeployModelStep;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.AfterClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.ALLOW_DELETE;
import static org.opensearch.flowframework.common.CommonValue.DEPROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.PIPELINE_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeprovisionWorkflowTransportActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool = new TestThreadPool(
        DeprovisionWorkflowTransportActionTests.class.getName(),
        new ScalingExecutorBuilder(
            DEPROVISION_WORKFLOW_THREAD_POOL,
            1,
            OpenSearchExecutors.allocatedProcessors(Settings.EMPTY),
            TimeValue.timeValueMinutes(5),
            FLOW_FRAMEWORK_THREAD_POOL_PREFIX + DEPROVISION_WORKFLOW_THREAD_POOL
        )
    );
    private Client client;
    private SdkClient sdkClient;
    private WorkflowStepFactory workflowStepFactory;
    private DeleteConnectorStep deleteConnectorStep;
    private UndeployModelStep undeployModelStep;
    private DeleteIndexStep deleteIndexStep;
    private DeleteIngestPipelineStep deleteIngestPipelineStep;
    private DeprovisionWorkflowTransportAction deprovisionWorkflowTransportAction;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private FlowFrameworkSettings flowFrameworkSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.client = mock(Client.class);
        this.sdkClient = SdkClientFactory.createSdkClient(client, NamedXContentRegistry.EMPTY, Collections.emptyMap());
        ThreadPool clientThreadPool = spy(threadPool);
        when(client.threadPool()).thenReturn(clientThreadPool);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);

        this.workflowStepFactory = mock(WorkflowStepFactory.class);
        this.deleteConnectorStep = mock(DeleteConnectorStep.class);
        when(this.workflowStepFactory.createStep(DeleteConnectorStep.NAME)).thenReturn(deleteConnectorStep);
        this.undeployModelStep = mock(UndeployModelStep.class);
        when(this.workflowStepFactory.createStep(UndeployModelStep.NAME)).thenReturn(undeployModelStep);
        this.deleteIndexStep = mock(DeleteIndexStep.class);
        when(this.deleteIndexStep.allowDeleteRequired()).thenReturn(true);
        when(this.workflowStepFactory.createStep(DeleteIndexStep.NAME)).thenReturn(deleteIndexStep);
        this.deleteIngestPipelineStep = mock(DeleteIngestPipelineStep.class);
        when(this.deleteIngestPipelineStep.allowDeleteRequired()).thenReturn(true);
        when(this.workflowStepFactory.createStep(DeleteIngestPipelineStep.NAME)).thenReturn(deleteIngestPipelineStep);

        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.getRequestTimeout()).thenReturn(TimeValue.timeValueSeconds(10));

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        this.deprovisionWorkflowTransportAction = new DeprovisionWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            clientThreadPool,
            client,
            sdkClient,
            workflowStepFactory,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings,
            clusterService,
            xContentRegistry(),
            Settings.EMPTY
        );
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(threadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testDeprovisionWorkflow() throws Exception {
        String workflowId = "1";

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        doAnswer(invocation -> {
            ActionListener<GetWorkflowStateResponse> responseListener = invocation.getArgument(2);

            WorkflowState state = WorkflowState.builder()
                .resourcesCreated(List.of(new ResourceCreated("create_connector", "step_1", CONNECTOR_ID, "connectorId")))
                .build();
            responseListener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(GetWorkflowStateAction.class), any(GetWorkflowStateRequest.class), any());

        doAnswer(invocation -> {
            Consumer<Boolean> booleanConsumer = invocation.getArgument(2);
            booleanConsumer.accept(Boolean.TRUE);
            return null;
        }).when(flowFrameworkIndicesHandler).doesTemplateExist(anyString(), any(), any(), any());

        PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();
        future.onResponse(WorkflowData.EMPTY);
        when(this.deleteConnectorStep.execute(anyString(), any(WorkflowData.class), anyMap(), anyMap(), anyMap(), nullable(String.class)))
            .thenReturn(future);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<WorkflowResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        deprovisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(workflowId, responseCaptor.getValue().getWorkflowId());
        verify(flowFrameworkIndicesHandler, times(1)).deleteResourceFromStateIndex(anyString(), any(ResourceCreated.class), any());
    }

    public void testFailToDeprovision() throws Exception {
        String workflowId = "1";

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        doAnswer(invocation -> {
            ActionListener<GetWorkflowStateResponse> responseListener = invocation.getArgument(2);

            WorkflowState state = WorkflowState.builder()
                .resourcesCreated(List.of(new ResourceCreated("deploy_model", "step_1", MODEL_ID, "modelId")))
                .build();
            responseListener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(GetWorkflowStateAction.class), any(GetWorkflowStateRequest.class), any());

        PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();
        future.onFailure(new RuntimeException("rte"));
        when(this.undeployModelStep.execute(anyString(), any(WorkflowData.class), anyMap(), anyMap(), anyMap(), nullable(String.class)))
            .thenReturn(future);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<WorkflowResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        deprovisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<FlowFrameworkException> exceptionCaptor = ArgumentCaptor.forClass(FlowFrameworkException.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(RestStatus.ACCEPTED, exceptionCaptor.getValue().getRestStatus());
        assertEquals("Failed to deprovision some resources: [model_id modelId].", exceptionCaptor.getValue().getMessage());
        verify(flowFrameworkIndicesHandler, times(0)).deleteResourceFromStateIndex(anyString(), any(ResourceCreated.class), any());
    }

    public void testAllowDeleteRequired() throws Exception {
        String workflowId = "1";

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<GetWorkflowStateResponse> responseListener = invocation.getArgument(2);

            WorkflowState state = WorkflowState.builder()
                .resourcesCreated(List.of(new ResourceCreated("create_index", "step_1", INDEX_NAME, "test-index")))
                .build();
            responseListener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(GetWorkflowStateAction.class), any(GetWorkflowStateRequest.class), any());

        doAnswer(invocation -> {
            Consumer<Boolean> booleanConsumer = invocation.getArgument(2);
            booleanConsumer.accept(Boolean.FALSE);
            return null;
        }).when(flowFrameworkIndicesHandler).doesTemplateExist(anyString(), any(), any(), any());

        // Test failure with no param
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<WorkflowResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        deprovisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<FlowFrameworkException> exceptionCaptor = ArgumentCaptor.forClass(FlowFrameworkException.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(RestStatus.FORBIDDEN, exceptionCaptor.getValue().getRestStatus());
        assertEquals(
            "These resources require the allow_delete parameter to deprovision: [index_name test-index].",
            exceptionCaptor.getValue().getMessage()
        );
        verify(flowFrameworkIndicesHandler, times(0)).deleteResourceFromStateIndex(anyString(), any(ResourceCreated.class), any());

        // Test (2nd) failure with wrong allow_delete param
        workflowRequest = new WorkflowRequest(workflowId, null, Map.of(ALLOW_DELETE, "wrong-index"));

        latch = new CountDownLatch(1);
        latchedActionListener = new LatchedActionListener<>(listener, latch);
        deprovisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(5, TimeUnit.SECONDS);

        exceptionCaptor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(listener, times(2)).onFailure(exceptionCaptor.capture());
        assertEquals(RestStatus.FORBIDDEN, exceptionCaptor.getValue().getRestStatus());
        assertEquals(
            "These resources require the allow_delete parameter to deprovision: [index_name test-index].",
            exceptionCaptor.getValue().getMessage()
        );
        verify(flowFrameworkIndicesHandler, times(0)).deleteResourceFromStateIndex(anyString(), any(ResourceCreated.class), any());

        // Test success with correct allow_delete param
        workflowRequest = new WorkflowRequest(workflowId, null, Map.of(ALLOW_DELETE, "wrong-index,test-index,other-index"));

        PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();
        future.onResponse(WorkflowData.EMPTY);
        when(this.deleteIndexStep.execute(anyString(), any(WorkflowData.class), anyMap(), anyMap(), anyMap(), nullable(String.class)))
            .thenReturn(future);

        latch = new CountDownLatch(1);
        latchedActionListener = new LatchedActionListener<>(listener, latch);
        deprovisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(workflowId, responseCaptor.getValue().getWorkflowId());
        verify(flowFrameworkIndicesHandler, times(1)).deleteResourceFromStateIndex(anyString(), any(ResourceCreated.class), any());
    }

    public void testFailToDeprovisionAndAllowDeleteRequired() throws Exception {
        String workflowId = "1";

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null, Map.of(ALLOW_DELETE, "wrong-index,test-pipeline"));

        doAnswer(invocation -> {
            ActionListener<GetWorkflowStateResponse> responseListener = invocation.getArgument(2);

            WorkflowState state = WorkflowState.builder()
                .resourcesCreated(
                    List.of(
                        new ResourceCreated("deploy_model", "step_1", MODEL_ID, "modelId"),
                        new ResourceCreated("create_index", "step_2", INDEX_NAME, "test-index"),
                        new ResourceCreated("create_ingest_pipeline", "step_3", PIPELINE_ID, "test-pipeline")
                    )
                )
                .build();
            responseListener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(GetWorkflowStateAction.class), any(GetWorkflowStateRequest.class), any());

        PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();
        future.onFailure(new RuntimeException("rte"));
        when(this.undeployModelStep.execute(anyString(), any(WorkflowData.class), anyMap(), anyMap(), anyMap(), nullable(String.class)))
            .thenReturn(future);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<WorkflowResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        deprovisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(5, TimeUnit.SECONDS);

        ArgumentCaptor<FlowFrameworkException> exceptionCaptor = ArgumentCaptor.forClass(FlowFrameworkException.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(RestStatus.ACCEPTED, exceptionCaptor.getValue().getRestStatus());
        assertEquals(
            "Failed to deprovision some resources: [pipeline_id test-pipeline, model_id modelId]."
                + " These resources require the allow_delete parameter to deprovision: [index_name test-index].",
            exceptionCaptor.getValue().getMessage()
        );
        verify(flowFrameworkIndicesHandler, times(0)).deleteResourceFromStateIndex(anyString(), any(ResourceCreated.class), any());
    }
}
