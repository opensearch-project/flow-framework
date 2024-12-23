/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.index.get.GetResult;
import org.opensearch.plugins.PluginsService;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.AfterClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProvisionWorkflowTransportActionTests extends OpenSearchTestCase {

    private static final TestThreadPool testThreadPool = new TestThreadPool(
        ProvisionWorkflowTransportActionTests.class.getName(),
        new ScalingExecutorBuilder(
            WORKFLOW_THREAD_POOL,
            1,
            Math.max(2, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(1),
            FLOW_FRAMEWORK_THREAD_POOL_PREFIX + WORKFLOW_THREAD_POOL
        ),
        new ScalingExecutorBuilder(
            PROVISION_WORKFLOW_THREAD_POOL,
            1,
            Math.max(4, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(5),
            FLOW_FRAMEWORK_THREAD_POOL_PREFIX + PROVISION_WORKFLOW_THREAD_POOL
        )
    );

    private Client client;
    private SdkClient sdkClient;
    private WorkflowProcessSorter workflowProcessSorter;
    private ProvisionWorkflowTransportAction provisionWorkflowTransportAction;
    private Template template;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private FlowFrameworkSettings flowFrameworkSettings;
    private EncryptorUtils encryptorUtils;
    private PluginsService pluginsService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.client = mock(Client.class);
        when(client.threadPool()).thenReturn(testThreadPool);
        this.sdkClient = SdkClientFactory.createSdkClient(client, NamedXContentRegistry.EMPTY, Collections.emptyMap());
        this.workflowProcessSorter = mock(WorkflowProcessSorter.class);
        this.flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        this.encryptorUtils = mock(EncryptorUtils.class);
        this.pluginsService = mock(PluginsService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        this.flowFrameworkIndicesHandler = spy(
            new FlowFrameworkIndicesHandler(client, sdkClient, clusterService, encryptorUtils, xContentRegistry())
        );

        this.provisionWorkflowTransportAction = new ProvisionWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            sdkClient,
            workflowProcessSorter,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings,
            encryptorUtils,
            pluginsService,
            clusterService,
            xContentRegistry(),
            Settings.EMPTY
        );

        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Collections.emptyMap(), Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Collections.emptyMap(), Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);

        this.template = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("provision", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null,
            null
        );
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testProvisionWorkflow() throws IOException, InterruptedException {

        String workflowId = "1";
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        this.template.toXContent(builder, null);
        BytesReference templateBytesRef = BytesReference.bytes(builder);
        GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, workflowId, 1, 1, 1, true, templateBytesRef, null, null);
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onResponse(new GetResponse(getResult));
        when(client.get(any(GetRequest.class))).thenReturn(future);

        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(template);

        // Bypass isWorkflowNotStarted and force true response
        doAnswer(invocation -> {
            Consumer<Optional<ProvisioningProgress>> progressConsumer = invocation.getArgument(1);
            progressConsumer.accept(Optional.of(ProvisioningProgress.NOT_STARTED));
            return null;
        }).when(flowFrameworkIndicesHandler).getProvisioningProgress(any(), any(), any());

        // Bypass updateFlowFrameworkSystemIndexDoc and stub on response
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mock(UpdateResponse.class));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), anyMap(), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(any(), any(Template.class), any(), anyBoolean());

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<WorkflowResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        provisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(workflowId, responseCaptor.getValue().getWorkflowId());
    }

    public void testProvisionWorkflowTwice() throws IOException, InterruptedException {

        String workflowId = "2";
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        this.template.toXContent(builder, null);
        BytesReference templateBytesRef = BytesReference.bytes(builder);
        GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, workflowId, 1, 1, 1, true, templateBytesRef, null, null);
        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onResponse(new GetResponse(getResult));
        when(client.get(any(GetRequest.class))).thenReturn(future);

        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(template);

        // Bypass isWorkflowNotStarted and force false response
        doAnswer(invocation -> {
            Consumer<Optional<ProvisioningProgress>> progressConsumer = invocation.getArgument(1);
            progressConsumer.accept(Optional.of(ProvisioningProgress.DONE));
            return null;
        }).when(flowFrameworkIndicesHandler).getProvisioningProgress(any(), any(), any());

        // Bypass updateFlowFrameworkSystemIndexDoc and stub on response
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mock(UpdateResponse.class));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), anyMap(), any());

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<WorkflowResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        provisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "The workflow provisioning state is DONE and can not be provisioned unless its state is NOT_STARTED: 2. Deprovision the workflow to reset the state.",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testFailedToRetrieveTemplateFromGlobalContext() throws InterruptedException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest request = new WorkflowRequest("1", null);

        PlainActionFuture<GetResponse> future = PlainActionFuture.newFuture();
        future.onFailure(new Exception("failed"));
        when(client.get(any(GetRequest.class))).thenReturn(future);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<WorkflowResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        provisionWorkflowTransportAction.doExecute(mock(Task.class), request, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to get template 1", exceptionCaptor.getValue().getMessage());
    }

}
