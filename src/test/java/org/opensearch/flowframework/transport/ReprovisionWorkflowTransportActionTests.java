/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStep;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.plugins.PluginsService;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.TestHelpers.deprecatedSettingWarning;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReprovisionWorkflowTransportActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private ActionFilters actionFilters;
    private Client client;
    private SdkClient sdkClient;
    private WorkflowStepFactory workflowStepFactory;
    private WorkflowProcessSorter workflowProcessSorter;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private FlowFrameworkSettings flowFrameworkSettings;
    private EncryptorUtils encryptorUtils;
    private PluginsService pluginsService;

    private ReprovisionWorkflowTransportAction reprovisionWorkflowTransportAction;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.transportService = mock(TransportService.class);
        this.actionFilters = mock(ActionFilters.class);
        this.client = mock(Client.class);
        this.sdkClient = SdkClientFactory.createSdkClient(client, NamedXContentRegistry.EMPTY, Collections.emptyMap());
        this.workflowStepFactory = mock(WorkflowStepFactory.class);
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

        ThreadPool clientThreadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);
        when(clientThreadPool.executor(anyString())).thenReturn(OpenSearchExecutors.newDirectExecutorService());

        this.reprovisionWorkflowTransportAction = new ReprovisionWorkflowTransportAction(
            transportService,
            actionFilters,
            clientThreadPool,
            client,
            sdkClient,
            workflowStepFactory,
            workflowProcessSorter,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings,
            encryptorUtils,
            pluginsService,
            clusterService,
            xContentRegistry(),
            Settings.EMPTY
        );

    }

    public void testReprovisionWorkflow() throws Exception {

        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<WorkflowState> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.COMPLETED.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            when(state.getError()).thenReturn(null);
            listener.onResponse(state);
            return null;
        }).when(flowFrameworkIndicesHandler).getWorkflowState(anyString(), nullable(String.class), any());

        // Stub reprovision sequence creation
        when(workflowProcessSorter.createReprovisionSequence(any(), any(), any(), any(), any())).thenReturn(
            List.of(mock(ProcessNode.class))
        );

        // Bypass updateFlowFrameworkSystemIndexDoc and stub on response
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(3);
            actionListener.onResponse(mock(UpdateResponse.class));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), nullable(String.class), anyMap(), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate, TimeValue.MINUS_ONE);

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(workflowId, responseCaptor.getValue().getWorkflowId());
        // The caller is authorized on the workflow, not necessarily on its state, so the state is read
        // as the plugin rather than through the user-facing transport action.
        verify(client, never()).execute(any(), any(GetWorkflowStateRequest.class), any());
    }

    public void testReprovisionProvisioningWorkflow() throws Exception {
        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<WorkflowState> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.PROVISIONING.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            listener.onResponse(state);
            return null;
        }).when(flowFrameworkIndicesHandler).getWorkflowState(anyString(), nullable(String.class), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate, TimeValue.MINUS_ONE);

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "The template can not be reprovisioned unless its provisioning state is DONE or FAILED: 1",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testReprovisionNotStartedWorkflow() throws Exception {
        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<WorkflowState> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.NOT_STARTED.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            listener.onResponse(state);
            return null;
        }).when(flowFrameworkIndicesHandler).getWorkflowState(anyString(), nullable(String.class), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate, TimeValue.MINUS_ONE);

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "The template can not be reprovisioned unless its provisioning state is DONE or FAILED: 1",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testFailedStateUpdate() throws Exception {
        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<WorkflowState> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.COMPLETED.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            when(state.getError()).thenReturn(null);
            listener.onResponse(state);
            return null;
        }).when(flowFrameworkIndicesHandler).getWorkflowState(anyString(), nullable(String.class), any());

        // Stub reprovision sequence creation
        when(workflowProcessSorter.createReprovisionSequence(any(), any(), any(), any(), any())).thenReturn(
            List.of(mock(ProcessNode.class))
        );

        // Bypass updateFlowFrameworkSystemIndexDoc and stub on response
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(3);
            actionListener.onFailure(new Exception("failed"));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), nullable(String.class), anyMap(), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate, TimeValue.MINUS_ONE);

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to update workflow state: 1", exceptionCaptor.getValue().getMessage());
    }

    public void testFailedWorkflowStateRetrieval() throws Exception {
        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state index retrieval failure
        doAnswer(invocation -> {

            ActionListener<WorkflowState> listener = invocation.getArgument(2);
            listener.onFailure(new Exception("failed"));
            return null;
        }).when(flowFrameworkIndicesHandler).getWorkflowState(anyString(), nullable(String.class), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate, TimeValue.MINUS_ONE);

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to get workflow state for workflow 1", exceptionCaptor.getValue().getMessage());
    }

    public void testReprovisionWorkflowExecutionException() throws Exception {
        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {
            ActionListener<WorkflowState> listener = invocation.getArgument(2);
            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.COMPLETED.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            when(state.getError()).thenReturn(null);
            listener.onResponse(state);
            return null;
        }).when(flowFrameworkIndicesHandler).getWorkflowState(anyString(), nullable(String.class), any());

        // Create a failed future for the workflow execution
        PlainActionFuture<WorkflowData> failedFuture = PlainActionFuture.newFuture();
        failedFuture.onFailure(new RuntimeException("Simulated failure during workflow execution"));
        ProcessNode failedProcessNode = mock(ProcessNode.class);
        when(failedProcessNode.execute()).thenReturn(failedFuture);
        WorkflowStep mockStep = mock(WorkflowStep.class);
        when(mockStep.getName()).thenReturn("FakeStep");
        when(failedProcessNode.workflowStep()).thenReturn(mockStep);

        // Stub reprovision sequence creation with the failed process node
        when(workflowProcessSorter.createReprovisionSequence(any(), any(), any(), any(), any())).thenReturn(List.of(failedProcessNode));

        // Bypass updateFlowFrameworkSystemIndexDoc and stub on response
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(3);
            actionListener.onResponse(mock(UpdateResponse.class));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), nullable(String.class), anyMap(), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(
            workflowId,
            mockTemplate,
            mockTemplate,
            TimeValue.timeValueSeconds(5)
        );

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().startsWith("Simulated failure during workflow execution"));
    }

    public void testReprovisionWorkflowSyncReadsStateAsThePlugin() throws Exception {
        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mock(Workflow.class));
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        doAnswer(invocation -> {
            ActionListener<WorkflowState> listener = invocation.getArgument(2);
            listener.onResponse(
                WorkflowState.builder().workflowId(workflowId).state(State.COMPLETED.toString()).resourcesCreated(List.of()).build()
            );
            return null;
        }).when(flowFrameworkIndicesHandler).getWorkflowState(anyString(), nullable(String.class), any());

        PlainActionFuture<WorkflowData> completed = PlainActionFuture.newFuture();
        completed.onResponse(WorkflowData.EMPTY);
        ProcessNode processNode = mock(ProcessNode.class);
        when(processNode.execute()).thenReturn(completed);
        WorkflowStep step = mock(WorkflowStep.class);
        when(step.getName()).thenReturn("FakeStep");
        when(processNode.workflowStep()).thenReturn(step);
        when(workflowProcessSorter.createReprovisionSequence(any(), any(), any(), any(), any())).thenReturn(List.of(processNode));

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(3);
            actionListener.onResponse(mock(UpdateResponse.class));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), nullable(String.class), anyMap(), any());
        when(client.threadPool().schedule(any(Runnable.class), any(TimeValue.class), anyString())).thenReturn(
            mock(Scheduler.ScheduledCancellable.class)
        );

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(
            workflowId,
            mockTemplate,
            mockTemplate,
            TimeValue.timeValueSeconds(5)
        );

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);

        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(State.COMPLETED.toString(), responseCaptor.getValue().getWorkflowState().getState());
        // Both the initial read and the read after completion go to the state index as the plugin.
        verify(flowFrameworkIndicesHandler, times(2)).getWorkflowState(anyString(), nullable(String.class), any());
        verify(client, never()).execute(any(), any(GetWorkflowStateRequest.class), any());
    }

    public void testReprovisionDeniedWhenBackendRolesMatchTemplateButNotState() throws Exception {
        ActionListener<WorkflowResponse> listener = reprovisionWithBackendRoleFilter("role-a", "role-b");

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        verify(listener, never()).onResponse(any());
        FlowFrameworkException exception = (FlowFrameworkException) exceptionCaptor.getValue();
        assertEquals(RestStatus.FORBIDDEN, exception.getRestStatus());
        assertEquals("User does not have permissions to access workflow: 1", exception.getMessage());
        // The template check passed, so the denial comes from the check on the state document.
        verify(client, times(1)).get(argThat(r -> GLOBAL_CONTEXT_INDEX.equals(r.index())), any());
        verify(client, times(1)).get(argThat(r -> WORKFLOW_STATE_INDEX.equals(r.index())), any());
        verify(flowFrameworkIndicesHandler, never()).getWorkflowState(anyString(), nullable(String.class), any());
        verify(flowFrameworkIndicesHandler, never()).updateFlowFrameworkSystemIndexDoc(any(), nullable(String.class), anyMap(), any());
    }

    public void testReprovisionReadsStateWhenBackendRolesMatchTemplateAndState() throws Exception {
        ActionListener<WorkflowResponse> listener = reprovisionWithBackendRoleFilter("role-a", "role-a");

        verify(listener, never()).onFailure(any());
        verify(client, times(1)).get(argThat(r -> WORKFLOW_STATE_INDEX.equals(r.index())), any());
        verify(flowFrameworkIndicesHandler, times(1)).getWorkflowState(eq("1"), nullable(String.class), any());
    }

    /**
     * Reprovisions workflow 1 as a user with backend role {@code role-a}, with backend role filtering on.
     *
     * @param templateRole the backend role of the user stored on the template
     * @param stateRole the backend role of the user stored on the workflow state
     * @return the listener the action reported to
     */
    private ActionListener<WorkflowResponse> reprovisionWithBackendRoleFilter(String templateRole, String stateRole) throws Exception {
        Settings settings = Settings.builder().put(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.of(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES))
        );
        ClusterState clusterState = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.hasIndex(anyString())).thenReturn(true);
        client.threadPool()
            .getThreadContext()
            .putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|role-a|engineering");

        Template template = Template.builder().name("test").user(new User("creator", List.of(templateRole), List.of(), List.of())).build();
        WorkflowState state = WorkflowState.builder()
            .workflowId("1")
            .state(State.COMPLETED.name())
            .user(new User("creator", List.of(stateRole), List.of(), List.of()))
            .build();
        doAnswer(invocation -> {
            GetRequest getRequest = invocation.getArgument(0);
            ActionListener<GetResponse> getListener = invocation.getArgument(1);
            ToXContentObject document = GLOBAL_CONTEXT_INDEX.equals(getRequest.index()) ? template : state;
            getListener.onResponse(TestHelpers.createGetResponse(document, "1", getRequest.index()));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        ReprovisionWorkflowTransportAction action = new ReprovisionWorkflowTransportAction(
            transportService,
            actionFilters,
            client.threadPool(),
            client,
            sdkClient,
            workflowStepFactory,
            workflowProcessSorter,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings,
            encryptorUtils,
            pluginsService,
            clusterService,
            xContentRegistry(),
            settings
        );
        allowedWarnings(deprecatedSettingWarning(FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES));

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        action.doExecute(mock(Task.class), new ReprovisionWorkflowRequest("1", template, template, TimeValue.MINUS_ONE), listener);
        return listener;
    }
}
