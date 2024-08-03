/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.plugins.PluginsService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReprovisionWorkflowTransportActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private ActionFilters actionFilters;
    private ThreadPool threadPool;
    private Client client;
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
        this.threadPool = mock(ThreadPool.class);
        this.client = mock(Client.class);
        this.workflowStepFactory = mock(WorkflowStepFactory.class);
        this.workflowProcessSorter = mock(WorkflowProcessSorter.class);
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        this.encryptorUtils = mock(EncryptorUtils.class);
        this.pluginsService = mock(PluginsService.class);

        this.reprovisionWorkflowTransportAction = new ReprovisionWorkflowTransportAction(
            transportService,
            actionFilters,
            threadPool,
            client,
            workflowStepFactory,
            workflowProcessSorter,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings,
            encryptorUtils,
            pluginsService
        );

        ThreadPool clientThreadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);
    }

    public void testReprovisionWorkflow() throws Exception {

        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<GetWorkflowStateResponse> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.COMPLETED.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            when(state.getError()).thenReturn(null);
            listener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(), any(GetWorkflowStateRequest.class), any());

        // Stub reprovision sequence creation
        when(workflowProcessSorter.createReprovisionSequence(any(), any(), any(), any())).thenReturn(List.of(mock(ProcessNode.class)));

        // Bypass updateFlowFrameworkSystemIndexDoc and stub on response
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(mock(UpdateResponse.class));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), any(), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate);

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(workflowId, responseCaptor.getValue().getWorkflowId());
    }

    public void testReprovisionProvisioningWorkflow() throws Exception {
        String workflowId = "1";

        Template mockTemplate = mock(Template.class);
        Workflow mockWorkflow = mock(Workflow.class);
        Map<String, Workflow> mockWorkflows = new HashMap<>();
        mockWorkflows.put(PROVISION_WORKFLOW, mockWorkflow);

        // Stub validations
        when(mockTemplate.workflows()).thenReturn(mockWorkflows);
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<GetWorkflowStateResponse> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.PROVISIONING.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            listener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(), any(GetWorkflowStateRequest.class), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate);

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
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<GetWorkflowStateResponse> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.NOT_STARTED.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            listener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(), any(GetWorkflowStateRequest.class), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate);

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
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state and resources created
        doAnswer(invocation -> {

            ActionListener<GetWorkflowStateResponse> listener = invocation.getArgument(2);

            WorkflowState state = mock(WorkflowState.class);
            ResourceCreated resourceCreated = new ResourceCreated("stepName", workflowId, "resourceType", "resourceId");
            when(state.getState()).thenReturn(State.COMPLETED.toString());
            when(state.resourcesCreated()).thenReturn(List.of(resourceCreated));
            when(state.getError()).thenReturn(null);
            listener.onResponse(new GetWorkflowStateResponse(state, true));
            return null;
        }).when(client).execute(any(), any(GetWorkflowStateRequest.class), any());

        // Stub reprovision sequence creation
        when(workflowProcessSorter.createReprovisionSequence(any(), any(), any(), any())).thenReturn(List.of(mock(ProcessNode.class)));

        // Bypass updateFlowFrameworkSystemIndexDoc and stub on response
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new Exception("failed"));
            return null;
        }).when(flowFrameworkIndicesHandler).updateFlowFrameworkSystemIndexDoc(any(), any(), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate);

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
        when(workflowProcessSorter.sortProcessNodes(any(), any(), any())).thenReturn(List.of());
        doNothing().when(workflowProcessSorter).validate(any(), any());
        when(encryptorUtils.decryptTemplateCredentials(any())).thenReturn(mockTemplate);

        // Stub state index retrieval failure
        doAnswer(invocation -> {

            ActionListener<GetWorkflowStateResponse> listener = invocation.getArgument(2);
            listener.onFailure(new Exception("failed"));
            return null;
        }).when(client).execute(any(), any(GetWorkflowStateRequest.class), any());

        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ReprovisionWorkflowRequest request = new ReprovisionWorkflowRequest(workflowId, mockTemplate, mockTemplate);

        reprovisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to get workflow state for workflow 1", exceptionCaptor.getValue().getMessage());
    }

}
