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
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.get.GetResult;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Assert;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GetWorkflowStateTransportActionTests extends OpenSearchTestCase {

    private static final TestThreadPool testThreadPool = new TestThreadPool(
        GetWorkflowStateTransportActionTests.class.getName(),
        new ScalingExecutorBuilder(
            WORKFLOW_THREAD_POOL,
            1,
            Math.max(2, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(1),
            FLOW_FRAMEWORK_THREAD_POOL_PREFIX + WORKFLOW_THREAD_POOL
        )
    );

    private GetWorkflowStateTransportAction getWorkflowStateTransportAction;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private FlowFrameworkSettings flowFrameworkSettings;
    private Client client;
    private SdkClient sdkClient;
    private NamedXContentRegistry xContentRegistry;
    private ActionListener<GetWorkflowStateResponse> response;
    private Task task;
    private EncryptorUtils encryptorUtils;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.xContentRegistry = mock(NamedXContentRegistry.class);
        this.flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        this.client = mock(Client.class);
        this.sdkClient = SdkClientFactory.createSdkClient(
            client,
            NamedXContentRegistry.EMPTY,
            Collections.emptyMap(),
            testThreadPool.executor(ThreadPool.Names.SAME)
        );
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        this.encryptorUtils = new EncryptorUtils(mock(ClusterService.class), client, sdkClient, xContentRegistry);
        this.flowFrameworkIndicesHandler = spy(
            new FlowFrameworkIndicesHandler(client, sdkClient, clusterService, encryptorUtils, xContentRegistry)
        );

        this.getWorkflowStateTransportAction = new GetWorkflowStateTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            flowFrameworkIndicesHandler,
            flowFrameworkSettings,
            client,
            sdkClient,
            xContentRegistry(),
            clusterService,
            Settings.EMPTY
        );
        task = Mockito.mock(Task.class);

        when(client.threadPool()).thenReturn(testThreadPool);

        response = new ActionListener<GetWorkflowStateResponse>() {
            @Override
            public void onResponse(GetWorkflowStateResponse getResponse) {
                assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {}
        };

    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testGetTransportAction() throws IOException {
        GetWorkflowStateRequest getWorkflowRequest = new GetWorkflowStateRequest("1234", false, null);
        getWorkflowStateTransportAction.doExecute(task, getWorkflowRequest, response);
    }

    public void testGetAction() {
        Assert.assertNotNull(GetWorkflowStateAction.INSTANCE.name());
        Assert.assertEquals(GetWorkflowStateAction.INSTANCE.name(), GetWorkflowStateAction.NAME);
    }

    public void testGetWorkflowStateRequest() throws IOException {
        GetWorkflowStateRequest request = new GetWorkflowStateRequest("1234", false, null);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetWorkflowStateRequest newRequest = new GetWorkflowStateRequest(input);
        Assert.assertEquals(request.getWorkflowId(), newRequest.getWorkflowId());
        Assert.assertEquals(request.getAll(), newRequest.getAll());
        Assert.assertNull(newRequest.validate());
    }

    public void testGetWorkflowStateResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        String workflowId = randomAlphaOfLength(5);
        WorkflowState workFlowState = new WorkflowState(
            workflowId,
            "test",
            "PROVISIONING",
            "IN_PROGRESS",
            Instant.now(),
            Instant.now(),
            TestHelpers.randomUser(),
            Collections.emptyMap(),
            Collections.emptyList(),
            null
        );

        GetWorkflowStateResponse response = new GetWorkflowStateResponse(workFlowState, false);
        response.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        GetWorkflowStateResponse newResponse = new GetWorkflowStateResponse(input);
        XContentBuilder builder = TestHelpers.builder();
        Assert.assertNotNull(newResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));

        Map<String, Object> map = TestHelpers.XContentBuilderToMap(builder);
        Assert.assertEquals(map.get("state"), workFlowState.getState());
        Assert.assertEquals(map.get("workflow_id"), workFlowState.getWorkflowId());
    }

    public void testExecuteGetWorkflowStateRequestFailure() throws IOException, InterruptedException {
        String workflowId = "test-workflow";
        GetWorkflowStateRequest request = new GetWorkflowStateRequest(workflowId, false, null);
        @SuppressWarnings("unchecked")
        ActionListener<GetWorkflowStateResponse> listener = mock(ActionListener.class);

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onFailure(new Exception("failed"));
        when(client.get(any(GetRequest.class))).thenReturn(future);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<GetWorkflowStateResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        getWorkflowStateTransportAction.doExecute(null, request, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        verify(listener, never()).onResponse(any(GetWorkflowStateResponse.class));
        ArgumentCaptor<FlowFrameworkException> responseCaptor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(listener, times(1)).onFailure(responseCaptor.capture());

        assertEquals("Failed to get workflow status of: " + workflowId, responseCaptor.getValue().getMessage());
    }

    public void testExecuteGetWorkflowStateRequestIndexNotFound() throws IOException, InterruptedException {
        String workflowId = "test-workflow";
        GetWorkflowStateRequest request = new GetWorkflowStateRequest(workflowId, false, null);
        @SuppressWarnings("unchecked")
        ActionListener<GetWorkflowStateResponse> listener = mock(ActionListener.class);

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onFailure(new IndexNotFoundException("index not found"));
        when(client.get(any(GetRequest.class))).thenReturn(future);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<GetWorkflowStateResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        getWorkflowStateTransportAction.doExecute(null, request, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        verify(listener, never()).onResponse(any(GetWorkflowStateResponse.class));
        ArgumentCaptor<FlowFrameworkException> responseCaptor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(listener, times(1)).onFailure(responseCaptor.capture());

        assertEquals("Fail to find workflow status of " + workflowId, responseCaptor.getValue().getMessage());
    }

    public void testExecuteGetWorkflowStateRequestParseFailure() throws IOException, InterruptedException {
        String workflowId = "test-workflow";
        GetWorkflowStateRequest request = new GetWorkflowStateRequest(workflowId, false, null);
        @SuppressWarnings("unchecked")
        ActionListener<GetWorkflowStateResponse> listener = mock(ActionListener.class);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        BytesReference templateBytesRef = BytesReference.bytes(builder);
        GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, workflowId, 1, 1, 1, true, templateBytesRef, null, null);

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onResponse(new GetResponse(getResult));
        when(client.get(any(GetRequest.class))).thenReturn(future);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<GetWorkflowStateResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        getWorkflowStateTransportAction.doExecute(null, request, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        verify(listener, never()).onResponse(any(GetWorkflowStateResponse.class));
        ArgumentCaptor<FlowFrameworkException> responseCaptor = ArgumentCaptor.forClass(FlowFrameworkException.class);
        verify(listener, times(1)).onFailure(responseCaptor.capture());

        assertEquals("Failed to get workflow status of: " + workflowId, responseCaptor.getValue().getMessage());
    }

}
