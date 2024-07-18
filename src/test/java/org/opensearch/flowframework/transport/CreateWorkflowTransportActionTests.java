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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.plugins.PluginsService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.UPDATE_WORKFLOW_FIELDS;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.CREATE_CONNECTOR;
import static org.opensearch.flowframework.common.WorkflowResources.DEPLOY_MODEL;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.REGISTER_REMOTE_MODEL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
    private FlowFrameworkSettings flowFrameworkSettings;
    private PluginsService pluginsService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);

        threadPool = mock(ThreadPool.class);
        this.flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.getMaxWorkflows()).thenReturn(2);
        when(flowFrameworkSettings.getRequestTimeout()).thenReturn(TimeValue.timeValueSeconds(10));
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
                flowFrameworkSettings,
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
            null
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
            null
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
        WorkflowRequest workflowRequest = new WorkflowRequest(null, template, new String[] { "off" }, false, Collections.emptyMap(), null, Collections.emptyMap());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> searchListener = invocation.getArgument(1);
            SearchResponse searchResponse = mock(SearchResponse.class);
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(3, TotalHits.Relation.EQUAL_TO), 1.0f);
            when(searchResponse.getHits()).thenReturn(searchHits);
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
            Collections.emptyMap(),
            null,
            Collections.emptyMap()
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
            null,
            Collections.emptyMap()
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
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(true);
            when(getResponse.getSourceAsString()).thenReturn(template.toJson());
            getListener.onResponse(getResponse);
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

    public void testFailedToUpdateNonExistingWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("2", template);

        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(false);
            getListener.onResponse(getResponse);
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
        assertEquals("Failed to retrieve template (2) from global context.", exceptionCaptor.getValue().getMessage());
    }

    public void testUpdateWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template);

        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(true);
            when(getResponse.getSourceAsString()).thenReturn(Template.builder().name("test").build().toJson());
            getListener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).updateTemplateInGlobalContext(anyString(), any(Template.class), any(), anyBoolean());

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

    public void testUpdateWorkflowWithField() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest(
            "1",
            Template.builder().name("new name").description("test").useCase(null).uiMetadata(Map.of("foo", "bar")).build(),
            Map.of(UPDATE_WORKFLOW_FIELDS, "true")
        );

        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(true);
            when(getResponse.getSourceAsString()).thenReturn(template.toJson());
            getListener.onResponse(getResponse);
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
        doAnswer(invocation -> {
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            GetResponse getResponse = mock(GetResponse.class);
            when(getResponse.isExists()).thenReturn(true);
            when(getResponse.getSourceAsString()).thenReturn(templateCaptor.getValue().toJson());
            getListener.onResponse(getResponse);
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
            null,
            Collections.emptyMap()
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
            Collections.emptyMap(),
            null,
            Collections.emptyMap()
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
        assertEquals("Provisioning failed.", exceptionCaptor.getValue().getMessage());
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
            TestHelpers.randomUser(),
            null,
            null,
            null
        );

        return validTemplate;
    }
}
