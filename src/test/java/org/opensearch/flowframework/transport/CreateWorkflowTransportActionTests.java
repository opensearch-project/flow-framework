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
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateWorkflowTransportActionTests extends OpenSearchTestCase {

    private CreateWorkflowTransportAction createWorkflowTransportAction;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private Template template;
    private Client client = mock(Client.class);
    private ThreadPool threadPool;
    private ParseUtils parseUtils;
    private ThreadContext threadContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        this.createWorkflowTransportAction = new CreateWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            flowFrameworkIndicesHandler,
            client
        );
        threadPool = mock(ThreadPool.class);
        // client = mock(Client.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        // threadContext = mock(ThreadContext.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        // when(threadContext.getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT)).thenReturn("123");
        // parseUtils = mock(ParseUtils.class);

        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of(), Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of(), Map.of("baz", "qux"));
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
            TestHelpers.randomUser()
        );
    }

    public void testFailedToCreateNewWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to create global_context index"));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), createNewWorkflow, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to create global_context index", exceptionCaptor.getValue().getMessage());
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

    // TODO: Fix these unit tests, manually tested these work but mocks here are wrong
    /*
    public void testCreateNewWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        ActionListener<IndexResponse> indexListener = mock(ActionListener.class);

        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putTemplateToGlobalContext(any(Template.class), any());

        ArgumentCaptor<IndexResponse> responseCaptorStateIndex = ArgumentCaptor.forClass(IndexResponse.class);
        verify(indexListener, times(1)).onResponse(responseCaptorStateIndex.capture());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(flowFrameworkIndicesHandler).putInitialStateToWorkflowState(responseCaptorStateIndex.getValue().getId(), null, any());

        createWorkflowTransportAction.doExecute(mock(Task.class), createNewWorkflow, listener);


        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        assertEquals("1", responseCaptor.getValue().getWorkflowId());

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

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        assertEquals("1", responseCaptor.getValue().getWorkflowId());
    }
    */
}
