/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.plugins.PluginsService;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.UPDATE_WORKFLOW_FIELDS;
import static org.opensearch.flowframework.common.CommonValue.WAIT_FOR_COMPLETION_TIMEOUT;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.CREATE_CONNECTOR;
import static org.opensearch.flowframework.common.WorkflowResources.DEPLOY_MODEL;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.REGISTER_REMOTE_MODEL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateWorkflowTransportActionTests extends OpenSearchTestCase {

    private CreateWorkflowTransportAction createWorkflowTransportAction;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private WorkflowProcessSorter workflowProcessSorter;
    private Template template;
    private Client client;
    private SdkClient sdkClient;
    private FlowFrameworkSettings flowFrameworkSettings;
    private PluginsService pluginsService;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        this.sdkClient = SdkClientFactory.createSdkClient(client, NamedXContentRegistry.EMPTY, Collections.emptyMap());

        this.flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.getMaxWorkflows()).thenReturn(2);
        when(flowFrameworkSettings.getRequestTimeout()).thenReturn(TimeValue.timeValueSeconds(10));
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        // Validation functionality should not be invoked in these unit tests, mocking instead
        this.workflowProcessSorter = mock(WorkflowProcessSorter.class);
        this.pluginsService = mock(PluginsService.class);

        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, Set.copyOf(List.of(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES)));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        ClusterName clusterName = new ClusterName("test");

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        final Settings.Builder existingSettings = Settings.builder().put(indexSettings).put(IndexMetadata.SETTING_INDEX_UUID, "test2UUID");

        IndexMetadata indexMetaData = IndexMetadata.builder(GLOBAL_CONTEXT_INDEX).settings(existingSettings).build();
        final Map<String, IndexMetadata> indices = new HashMap<>();
        indices.put(GLOBAL_CONTEXT_INDEX, indexMetaData);
        ClusterState clusterState = ClusterState.builder(clusterName).metadata(Metadata.builder().indices(indices).build()).build();
        when(clusterService.state()).thenReturn(clusterState);

        // Spy this action to stub check max workflows
        this.createWorkflowTransportAction = spy(
            new CreateWorkflowTransportAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                workflowProcessSorter,
                flowFrameworkIndicesHandler,
                flowFrameworkSettings,
                client,
                sdkClient,
                pluginsService,
                clusterService,
                xContentRegistry(),
                Settings.EMPTY
            )
        );

        ThreadPool threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        Workflow workflow = TestHelpers.createSampleWorkflow();

        this.template = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null,
            null,
            Collections.emptyList()
        );
    }

    public void testValidation_withoutProvision_Success() {
        Template validTemplate = generateValidTemplate();

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, validTemplate);
        createWorkflowTransportAction.doExecute(mock(Task.class), createNewWorkflow, listener);
    }

    public void testValidation_Failed() throws Exception {

        WorkflowNode createConnector = new WorkflowNode(
            "workflow_step_1",
            "create_connector",
            Collections.emptyMap(),
            Map.ofEntries(
                Map.entry("name", ""),
                Map.entry("description", ""),
                Map.entry("version", ""),
                Map.entry("protocol", ""),
                Map.entry("parameters", ""),
                Map.entry("credential", ""),
                Map.entry("actions", "")
            )
        );

        WorkflowNode registerModel = new WorkflowNode(
            "workflow_step_2",
            "register_model",
            Map.ofEntries(Map.entry("workflow_step_1", CONNECTOR_ID)),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );

        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            "deploy_model",
            Map.ofEntries(Map.entry("workflow_step_2", MODEL_ID)),
            Collections.emptyMap()
        );

        WorkflowEdge edge1 = new WorkflowEdge(createConnector.id(), registerModel.id());
        WorkflowEdge edge2 = new WorkflowEdge(registerModel.id(), deployModel.id());
        WorkflowEdge cyclicalEdge = new WorkflowEdge(deployModel.id(), createConnector.id());

        Workflow workflow = new Workflow(
            Collections.emptyMap(),
            List.of(createConnector, registerModel, deployModel),
            List.of(edge1, edge2, cyclicalEdge)
        );

        Template cyclicalTemplate = new Template(
            "test",
            "description",
            "use case",
            Version.fromString("1.0.0"),
            List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0")),
            Map.of("workflow", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null,
            null,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        // Stub validation failure
        doThrow(Exception.class).when(workflowProcessSorter).validate(any(), any());
        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, cyclicalTemplate);

        createWorkflowTransportAction.doExecute(mock(Task.class), createNewWorkflow, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testMaxWorkflow() {
        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(true);

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            false,
            null
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> searchListener = invocation.getArgument(1);
            SearchResponse searchResponse = generateEmptySearchResponseWithHitCount(3);
            searchListener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(SearchRequest.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(("Maximum workflows limit reached: 2"), exceptionCaptor.getValue().getMessage());
    }

    public void testMaxWorkflowWithNoIndex() {
        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(false);

        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean booleanResponse) {
                assertTrue(booleanResponse);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should call onResponse");
            }
        };
        createWorkflowTransportAction.checkMaxWorkflows(new TimeValue(10, TimeUnit.SECONDS), Integer.valueOf(10), "tenant-id", listener);
    }

    public void testFailedToCreateNewWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            false,
            null
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(3);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), any(Integer.class), nullable(String.class), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(1);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(nullable(String.class), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("failed"));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to save use case template", exceptionCaptor.getValue().getMessage());
    }

    public void testCreateNewWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            false,
            null
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(3);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), any(Integer.class), nullable(String.class), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(1);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(nullable(String.class), any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(4);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any(), any(), any());

        ArgumentCaptor<WorkflowResponse> workflowResponseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        verify(listener, times(1)).onResponse(workflowResponseCaptor.capture());

        assertEquals("1", workflowResponseCaptor.getValue().getWorkflowId());
    }

    public void testCreateWithUserAndFilterOn() {
        Settings settings = Settings.builder().put(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        CreateWorkflowTransportAction createWorkflowTransportAction1 = spy(
            new CreateWorkflowTransportAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                workflowProcessSorter,
                flowFrameworkIndicesHandler,
                flowFrameworkSettings,
                client,
                sdkClient,
                pluginsService,
                clusterService,
                xContentRegistry(),
                settings
            )
        );

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            false,
            null
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(3);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), any(Integer.class), nullable(String.class), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(1);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(nullable(String.class), any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(4);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any(), any(), any());

        ArgumentCaptor<WorkflowResponse> workflowResponseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);

        createWorkflowTransportAction1.doExecute(mock(Task.class), workflowRequest, listener);

        verify(listener, times(1)).onResponse(workflowResponseCaptor.capture());
    }

    public void testFailedToCreateNewWorkflowWithNullUser() {
        Settings settings = Settings.builder().put(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, null);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        CreateWorkflowTransportAction createWorkflowTransportAction1 = spy(
            new CreateWorkflowTransportAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                workflowProcessSorter,
                flowFrameworkIndicesHandler,
                flowFrameworkSettings,
                client,
                sdkClient,
                pluginsService,
                clusterService,
                xContentRegistry(),
                settings
            )
        );

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            false,
            null
        );

        createWorkflowTransportAction1.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Filter by backend roles is enabled and User is null", exceptionCaptor.getValue().getMessage());
    }

    public void testFailedToCreateNewWorkflowWithNoBackendRoleUser() {
        Settings settings = Settings.builder().put(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "test");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        CreateWorkflowTransportAction createWorkflowTransportAction1 = spy(
            new CreateWorkflowTransportAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                workflowProcessSorter,
                flowFrameworkIndicesHandler,
                flowFrameworkSettings,
                client,
                sdkClient,
                pluginsService,
                clusterService,
                xContentRegistry(),
                settings
            )
        );

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            false,
            null
        );

        createWorkflowTransportAction1.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "Filter by backend roles is enabled, but User test does not have any backend roles configured",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testUpdateWorkflowWithReprovision() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            "1",
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            true,
            null
        );

        GetResponse getWorkflowResponse = TestHelpers.createGetResponse(template, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new WorkflowResponse("1"));
            return null;
        }).when(client).execute(any(), any(), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        assertEquals("1", responseCaptor.getValue().getWorkflowId());
    }

    public void testHandleReprovisionWithMinusOneTimeout() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        // Create a request with TimeValue.MINUS_ONE for waitForCompletionTimeout
        WorkflowRequest workflowRequest = new WorkflowRequest(
            "1",
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            true,
            null
        );

        GetResponse getWorkflowResponse = TestHelpers.createGetResponse(template, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        // Mock the reprovision response with a workflow ID but no state
        doAnswer(invocation -> {
            ReprovisionWorkflowRequest reprovisionRequest = invocation.getArgument(1);
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);

            // Verify that the reprovision request has TimeValue.MINUS_ONE
            assertEquals(TimeValue.MINUS_ONE, reprovisionRequest.getWaitForCompletionTimeout());

            // Return a response with just the workflow ID
            WorkflowResponse reprovisionResponse = new WorkflowResponse("1");
            responseListener.onResponse(reprovisionResponse);
            return null;
        }).when(client).execute(eq(ReprovisionWorkflowAction.INSTANCE), any(ReprovisionWorkflowRequest.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        // Verify that the response only contains the workflow ID
        assertEquals("1", responseCaptor.getValue().getWorkflowId());
        assertNull(responseCaptor.getValue().getWorkflowState());
    }

    public void testHandleReprovisionWithSpecifiedTimeout() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        // Create a request with a specific timeout value
        WorkflowRequest workflowRequest = new WorkflowRequest(
            "1",
            template,
            new String[] { "off" },
            false,
            Map.of(WAIT_FOR_COMPLETION_TIMEOUT, "5s"),
            true,
            null
        );

        GetResponse getWorkflowResponse = TestHelpers.createGetResponse(template, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        // Create a mock workflow state for the response
        WorkflowState mockState = new WorkflowState(
            "1",
            "test",
            "PROVISIONING",
            "IN_PROGRESS",
            Instant.now(),
            Instant.now(),
            TestHelpers.randomUser(),
            Collections.emptyMap(),
            Collections.emptyList(),
            null,
            Collections.emptyList()
        );

        // Mock the reprovision response with both workflow ID and state
        doAnswer(invocation -> {
            ReprovisionWorkflowRequest reprovisionRequest = invocation.getArgument(1);
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);

            // Verify that the reprovision request has the specified timeout
            assertEquals(TimeValue.timeValueSeconds(5), reprovisionRequest.getWaitForCompletionTimeout());

            // Return a response with both workflow ID and state
            WorkflowResponse reprovisionResponse = new WorkflowResponse("1", mockState);
            responseListener.onResponse(reprovisionResponse);
            return null;
        }).when(client).execute(eq(ReprovisionWorkflowAction.INSTANCE), any(ReprovisionWorkflowRequest.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        // Verify that the response contains both workflow ID and state
        assertEquals("1", responseCaptor.getValue().getWorkflowId());
        assertNotNull(responseCaptor.getValue().getWorkflowState());
        assertEquals("PROVISIONING", responseCaptor.getValue().getWorkflowState().getState());
    }

    public void testFailedToUpdateWorkflowWithReprovision() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            "1",
            template,
            new String[] { "off" },
            false,
            Collections.emptyMap(),
            true,
            null
        );

        GetResponse getWorkflowResponse = TestHelpers.createGetResponse(template, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            responseListener.onFailure(new Exception("failed"));
            return null;
        }).when(client).execute(any(), any(), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<Exception> responseCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(responseCaptor.capture());

        assertEquals("Reprovisioning failed for workflow 1", responseCaptor.getValue().getMessage());
    }

    public void testFailedToUpdateWorkflow() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template);

        GetResponse getWorkflowResponse = TestHelpers.createGetResponse(template, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onFailure(new Exception("failed"));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(anyString(), any(Template.class), any(), anyBoolean());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to update use case template 1", exceptionCaptor.getValue().getMessage());
    }

    public void testFailedToUpdateNonExistingWorkflow() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("2", template);

        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onFailure(new Exception("test"));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onFailure(new Exception("failed"));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(any(), any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to get data object from index .plugins-flow-framework-templates", exceptionCaptor.getValue().getMessage());
    }

    public void testUpdateWorkflow() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(anyString(), any(Template.class), any(), anyBoolean());

        GetResponse getWorkflowResponse = TestHelpers.createGetResponse(template, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(3);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(anyString(), nullable(String.class), anyMap(), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        assertEquals("1", responseCaptor.getValue().getWorkflowId());
    }

    public void testUpdateWorkflowWithField() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        Template template1 = Template.builder().name("new name").description("test").useCase(null).uiMetadata(Map.of("foo", "bar")).build();

        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template1, Map.of(UPDATE_WORKFLOW_FIELDS, "true"));

        GetResponse getWorkflowResponse = TestHelpers.createGetResponse(template, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(anyString(), any(Template.class), any(), anyBoolean());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);

        verify(listener, times(1)).onResponse(any());

        ArgumentCaptor<Template> templateCaptor = ArgumentCaptor.forClass(Template.class);
        verify(flowFrameworkIndicesHandler, times(1)).updateTemplateInGlobalContext(
            anyString(),
            templateCaptor.capture(),
            any(),
            anyBoolean()
        );
        assertEquals("new name", templateCaptor.getValue().name());
        assertEquals("test", templateCaptor.getValue().description());
        assertEquals(template.useCase(), templateCaptor.getValue().useCase());
        assertEquals(template.templateVersion(), templateCaptor.getValue().templateVersion());
        assertEquals(template.compatibilityVersion(), templateCaptor.getValue().compatibilityVersion());
        assertEquals(Map.of("foo", "bar"), templateCaptor.getValue().getUiMetadata());

        updateWorkflow = new WorkflowRequest(
            "1",
            Template.builder()
                .useCase("foo")
                .templateVersion(Version.CURRENT)
                .compatibilityVersion(List.of(Version.V_2_0_0, Version.CURRENT))
                .build(),
            Map.of(UPDATE_WORKFLOW_FIELDS, "true")
        );

        GetResponse getWorkflowResponse1 = TestHelpers.createGetResponse(template1, "123", GLOBAL_CONTEXT_INDEX);
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            getListener.onResponse(getWorkflowResponse1);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);
        verify(listener, times(2)).onResponse(any());

        ArgumentCaptor<Template> newTemplateCaptor = ArgumentCaptor.forClass(Template.class);
        verify(flowFrameworkIndicesHandler, times(2)).updateTemplateInGlobalContext(
            anyString(),
            newTemplateCaptor.capture(),
            any(),
            anyBoolean()
        );
        assertEquals("new name", newTemplateCaptor.getValue().name());
        assertEquals("test", newTemplateCaptor.getValue().description());
        assertEquals("foo", newTemplateCaptor.getValue().useCase());
        assertEquals(Version.CURRENT, newTemplateCaptor.getValue().templateVersion());
        assertEquals(List.of(Version.V_2_0_0, Version.CURRENT), newTemplateCaptor.getValue().compatibilityVersion());
        assertEquals(Map.of("foo", "bar"), newTemplateCaptor.getValue().getUiMetadata());
    }

    public void testCreateWorkflow_withValidation_withProvision_Success() throws Exception {

        Template validTemplate = generateValidTemplate();

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        doNothing().when(workflowProcessSorter).validate(any(), any());
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            validTemplate,
            new String[] { "all" },
            true,
            Collections.emptyMap(),
            false,
            null
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(3);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), any(Integer.class), nullable(String.class), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(1);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(nullable(String.class), any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(4);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            WorkflowResponse response = mock(WorkflowResponse.class);
            when(response.getWorkflowId()).thenReturn("1");
            responseListener.onResponse(response);
            return null;
        }).when(client).execute(eq(ProvisionWorkflowAction.INSTANCE), any(), any());

        ArgumentCaptor<WorkflowResponse> workflowResponseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        verify(listener, times(1)).onResponse(workflowResponseCaptor.capture());
        assertEquals("1", workflowResponseCaptor.getValue().getWorkflowId());
    }

    public void testCreateWorkflow_withValidation_withWaitForCompletion_withProvision_Success() throws Exception {

        Template validTemplate = generateValidTemplate();

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        doNothing().when(workflowProcessSorter).validate(any(), any());
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            validTemplate,
            new String[] { "all" },
            true,
            Map.of(WAIT_FOR_COMPLETION_TIMEOUT, "5s"),
            false
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(3);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), any(Integer.class), nullable(String.class), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(1);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(nullable(String.class), any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(4);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            WorkflowResponse response = mock(WorkflowResponse.class);
            when(response.getWorkflowId()).thenReturn("1");
            when(response.getWorkflowState()).thenReturn(
                new WorkflowState(
                    "1",
                    "test",
                    "PROVISIONING",
                    "IN_PROGRESS",
                    Instant.now(),
                    Instant.now(),
                    TestHelpers.randomUser(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    null,
                    Collections.emptyList()
                )
            );
            responseListener.onResponse(response);
            return null;
        }).when(client).execute(eq(ProvisionWorkflowAction.INSTANCE), any(WorkflowRequest.class), any(ActionListener.class));

        ArgumentCaptor<WorkflowResponse> workflowResponseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        verify(listener, times(1)).onResponse(workflowResponseCaptor.capture());
        assertEquals("1", workflowResponseCaptor.getValue().getWorkflowId());
        assertEquals("PROVISIONING", workflowResponseCaptor.getValue().getWorkflowState().getState());
    }

    public void testCreateWorkflow_withValidation_withWaitForCompletionTimeSetZero_withProvision_Success() throws Exception {

        Template validTemplate = generateValidTemplate();

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);

        doNothing().when(workflowProcessSorter).validate(any(), any());
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            validTemplate,
            new String[] { "all" },
            true,
            Map.of(WAIT_FOR_COMPLETION_TIMEOUT, "0s"),
            false
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(3);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), any(Integer.class), nullable(String.class), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(1);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(nullable(String.class), any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(4);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            WorkflowResponse response = mock(WorkflowResponse.class);
            when(response.getWorkflowId()).thenReturn("1");
            when(response.getWorkflowState()).thenReturn(
                new WorkflowState(
                    "1",
                    "test",
                    "PROVISIONING",
                    "IN_PROGRESS",
                    Instant.now(),
                    Instant.now(),
                    TestHelpers.randomUser(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    null,
                    Collections.emptyList()
                )
            );
            responseListener.onResponse(response);
            return null;
        }).when(client).execute(eq(ProvisionWorkflowAction.INSTANCE), any(WorkflowRequest.class), any(ActionListener.class));

        ArgumentCaptor<WorkflowResponse> workflowResponseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        verify(listener, times(1)).onResponse(workflowResponseCaptor.capture());
        assertEquals("1", workflowResponseCaptor.getValue().getWorkflowId());
        assertEquals("PROVISIONING", workflowResponseCaptor.getValue().getWorkflowState().getState());
    }

    public void testCreateWorkflow_withValidation_withProvision_FailedProvisioning() throws Exception {

        Template validTemplate = generateValidTemplate();

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        doNothing().when(workflowProcessSorter).validate(any(), any());
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            validTemplate,
            new String[] { "all" },
            true,
            Collections.emptyMap(),
            false,
            null
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(3);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), any(Integer.class), nullable(String.class), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(1);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(nullable(String.class), any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(4);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            WorkflowResponse response = mock(WorkflowResponse.class);
            when(response.getWorkflowId()).thenReturn("1");
            responseListener.onFailure(new Exception("failed"));
            return null;
        }).when(client).execute(eq(ProvisionWorkflowAction.INSTANCE), any(), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Provisioning failed.", exceptionCaptor.getValue().getMessage());
    }

    private Template generateValidTemplate() {
        WorkflowNode createConnector = new WorkflowNode(
            "workflow_step_1",
            CREATE_CONNECTOR.getCreateStep(),
            Collections.emptyMap(),
            Map.ofEntries(
                Map.entry("name", ""),
                Map.entry("description", ""),
                Map.entry("version", ""),
                Map.entry("protocol", ""),
                Map.entry("parameters", ""),
                Map.entry("credential", ""),
                Map.entry("actions", "")
            )
        );
        WorkflowNode registerModel = new WorkflowNode(
            "workflow_step_2",
            REGISTER_REMOTE_MODEL.getCreateStep(),
            Map.ofEntries(Map.entry("workflow_step_1", CONNECTOR_ID)),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            DEPLOY_MODEL.getCreateStep(),
            Map.ofEntries(Map.entry("workflow_step_2", MODEL_ID)),
            Collections.emptyMap()
        );

        WorkflowEdge edge1 = new WorkflowEdge(createConnector.id(), registerModel.id());
        WorkflowEdge edge2 = new WorkflowEdge(registerModel.id(), deployModel.id());

        Workflow workflow = new Workflow(
            Collections.emptyMap(),
            List.of(createConnector, registerModel, deployModel),
            List.of(edge1, edge2)
        );

        Template validTemplate = new Template(
            "test",
            "description",
            "use case",
            Version.fromString("1.0.0"),
            List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0")),
            Map.of("workflow", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null,
            null,
            Collections.emptyList()
        );

        return validTemplate;
    }

    /**
     * Generates a parseable SearchResponse with a hit count but no hits (size=0)
     * @param hitCount number of hits
     * @return a parseable SearchResponse
     */
    private SearchResponse generateEmptySearchResponseWithHitCount(int hitCount) {
        SearchHit[] hits = new SearchHit[0];
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponseSections searchSections = new SearchResponseSections(
            searchHits,
            InternalAggregations.EMPTY,
            null,
            true,
            false,
            null,
            1
        );
        SearchResponse searchResponse = new SearchResponse(
            searchSections,
            null,
            1,
            1,
            0,
            11,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        return searchResponse;
    }
}
