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
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
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
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProvisionWorkflowTransportActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
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
        this.threadPool = mock(ThreadPool.class);
        this.client = mock(Client.class);
        this.sdkClient = SdkClientFactory.createSdkClient(client, NamedXContentRegistry.EMPTY, Collections.emptyMap());
        this.workflowProcessSorter = mock(WorkflowProcessSorter.class);
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        this.flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        this.encryptorUtils = mock(EncryptorUtils.class);
        this.pluginsService = mock(PluginsService.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        this.provisionWorkflowTransportAction = new ProvisionWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            threadPool,
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
            null
        );

        ThreadPool clientThreadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);
    }

    public void testProvisionWorkflow() {

        String workflowId = "1";
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        // Bypass client.get and stub success case
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            this.template.toXContent(builder, null);
            BytesReference templateBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, workflowId, 1, 1, 1, true, templateBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());

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

        provisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(workflowId, responseCaptor.getValue().getWorkflowId());
    }

    public void testProvisionWorkflowTwice() {

        String workflowId = "2";
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        // Bypass client.get and stub success case
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            this.template.toXContent(builder, null);
            BytesReference templateBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, workflowId, 1, 1, 1, true, templateBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());

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

        provisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "The workflow provisioning state is DONE and can not be provisioned unless its state is NOT_STARTED: 2. Deprovision the workflow to reset the state.",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testFailedToRetrieveTemplateFromGlobalContext() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest request = new WorkflowRequest("1", null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("failed"));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        provisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to retrieve template from global context for workflow 1", exceptionCaptor.getValue().getMessage());
    }

}
