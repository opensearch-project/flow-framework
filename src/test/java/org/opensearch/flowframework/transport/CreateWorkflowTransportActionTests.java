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
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.plugins.PluginsService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.ArgumentCaptor;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOW_STEPS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.TASK_REQUEST_RETRY_DURATION;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.CREATE_CONNECTOR;
import static org.opensearch.flowframework.common.WorkflowResources.DEPLOY_MODEL;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.REGISTER_REMOTE_MODEL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
    private ThreadPool threadPool;
    private ClusterSettings clusterSettings;
    private ClusterService clusterService;
    private Settings settings;
    private PluginsService pluginsService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);

        threadPool = mock(ThreadPool.class);
        settings = Settings.builder()
            .put("plugins.flow_framework.max_workflows", 2)
            .put("plugins.flow_framework.request_timeout", TimeValue.timeValueSeconds(10))
            .build();
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(FLOW_FRAMEWORK_ENABLED, MAX_WORKFLOWS, MAX_WORKFLOW_STEPS, WORKFLOW_REQUEST_TIMEOUT, TASK_REQUEST_RETRY_DURATION)
        ).collect(Collectors.toSet());
        clusterSettings = new ClusterSettings(settings, settingsSet);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        // Validation functionality should not be invoked in these unit tests, mocking instead
        this.workflowProcessSorter = mock(WorkflowProcessSorter.class);
        this.pluginsService = mock(PluginsService.class);

        // Spy this action to stub check max workflows
        this.createWorkflowTransportAction = spy(
            new CreateWorkflowTransportAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                workflowProcessSorter,
                flowFrameworkIndicesHandler,
                settings,
                client,
                pluginsService
            )
        );
        // client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        // threadContext = mock(ThreadContext.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        // when(threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)).thenReturn("123");
        // parseUtils = mock(ParseUtils.class);

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
            Map.of("workflow", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser()
        );
    }

    public void testValidation_withoutProvision_Success() {
        Template validTemplate = generateValidTemplate();

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, validTemplate, new String[] { "all" }, false, null, null);
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
            TestHelpers.randomUser()
        );

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        // Stub validation failure
        doThrow(Exception.class).when(workflowProcessSorter).validate(any(), any());
        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, cyclicalTemplate, new String[] { "all" }, false, null, null);

        createWorkflowTransportAction.doExecute(mock(Task.class), createNewWorkflow, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testMaxWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            WORKFLOW_REQUEST_TIMEOUT.get(settings),
            MAX_WORKFLOWS.get(settings)
        );

        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(2);
            checkMaxWorkflowListener.onResponse(false);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), anyInt(), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(("Maximum workflows limit reached 2"), exceptionCaptor.getValue().getMessage());
    }

    public void testMaxWorkflowWithNoIndex() {
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean booleanResponse) {
                assertTrue(booleanResponse);
            }

            @Override
            public void onFailure(Exception e) {}
        };
        createWorkflowTransportAction.checkMaxWorkflows(new TimeValue(10, TimeUnit.SECONDS), 10, listener);
    }

    public void testFailedToCreateNewWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            WORKFLOW_REQUEST_TIMEOUT.get(settings),
            MAX_WORKFLOWS.get(settings)
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(2);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), anyInt(), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(0);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to create global_context index"));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to create global_context index", exceptionCaptor.getValue().getMessage());
    }

    public void testCreateNewWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(
            null,
            template,
            new String[] { "off" },
            false,
            WORKFLOW_REQUEST_TIMEOUT.get(settings),
            MAX_WORKFLOWS.get(settings)
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(2);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), anyInt(), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(0);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any());

        ArgumentCaptor<WorkflowResponse> workflowResponseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        verify(listener, times(1)).onResponse(workflowResponseCaptor.capture());

        assertEquals("1", workflowResponseCaptor.getValue().getWorkflowId());
    }

    public void testFailedToUpdateWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onFailure(new Exception("Failed to update use case template"));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(any(), any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to update use case template", exceptionCaptor.getValue().getMessage());
    }

    public void testUpdateWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(any(), any(Template.class), any());

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(2);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(anyString(), any(), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        assertEquals("1", responseCaptor.getValue().getWorkflowId());
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
            WORKFLOW_REQUEST_TIMEOUT.get(settings),
            MAX_WORKFLOWS.get(settings)
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(2);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), anyInt(), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(0);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            WorkflowResponse response = mock(WorkflowResponse.class);
            when(response.getWorkflowId()).thenReturn("1");
            responseListener.onResponse(response);
            return null;
        }).when(client).execute(eq(ProvisionWorkflowAction.INSTANCE), any(WorkflowRequest.class), any(ActionListener.class));

        ArgumentCaptor<WorkflowResponse> workflowResponseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        verify(listener, times(1)).onResponse(workflowResponseCaptor.capture());
        assertEquals("1", workflowResponseCaptor.getValue().getWorkflowId());
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
            WORKFLOW_REQUEST_TIMEOUT.get(settings),
            MAX_WORKFLOWS.get(settings)
        );

        // Bypass checkMaxWorkflows and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> checkMaxWorkflowListener = invocation.getArgument(2);
            checkMaxWorkflowListener.onResponse(true);
            return null;
        }).when(createWorkflowTransportAction).checkMaxWorkflows(any(TimeValue.class), anyInt(), any());

        // Bypass initializeConfigIndex and force onResponse
        doAnswer(invocation -> {
            ActionListener<Boolean> initalizeMasterKeyIndexListener = invocation.getArgument(0);
            initalizeMasterKeyIndexListener.onResponse(true);
            return null;
        }).when(flowFrameworkIndicesHandler).initializeConfigIndex(any());

        // Bypass putTemplateToGlobalContext and force onResponse
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(), any());

        // Bypass putInitialStateToWorkflowState and force on response
        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(any(), any(), any());

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            WorkflowResponse response = mock(WorkflowResponse.class);
            when(response.getWorkflowId()).thenReturn("1");
            responseListener.onFailure(new Exception("failed"));
            return null;
        }).when(client).execute(eq(ProvisionWorkflowAction.INSTANCE), any(WorkflowRequest.class), any(ActionListener.class));

        createWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("failed", exceptionCaptor.getValue().getMessage());
    }

    private Template generateValidTemplate() {
        WorkflowNode createConnector = new WorkflowNode(
            "workflow_step_1",
            CREATE_CONNECTOR.getWorkflowStep(),
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
            REGISTER_REMOTE_MODEL.getWorkflowStep(),
            Map.ofEntries(Map.entry("workflow_step_1", CONNECTOR_ID)),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            DEPLOY_MODEL.getWorkflowStep(),
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
            TestHelpers.randomUser()
        );

        return validTemplate;
    }
}
