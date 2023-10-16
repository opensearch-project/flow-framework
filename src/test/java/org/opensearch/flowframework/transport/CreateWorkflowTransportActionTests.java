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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.flowframework.indices.GlobalContextHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CreateWorkflowTransportActionTests extends OpenSearchTestCase {

    private CreateWorkflowTransportAction createWorkflowTransportAction;
    private GlobalContextHandler globalContextHandler;
    private Template template;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.globalContextHandler = mock(GlobalContextHandler.class);
        this.createWorkflowTransportAction = new CreateWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            globalContextHandler
        );

        List<String> operations = List.of("operation");
        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);

        this.template = new Template(
            "test",
            "description",
            "use case",
            operations,
            templateVersion,
            compatibilityVersions,
            Map.ofEntries(Map.entry("userKey", "userValue"), Map.entry("userMapKey", Map.of("nestedKey", "nestedValue"))),
            Map.of("workflow", workflow),
            Map.of("outputKey", "outputValue"),
            Map.of("resourceKey", "resourceValue")
        );
    }

    public void testCreateNewWorkflow() {

        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(globalContextHandler).putTemplateToGlobalContext(any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), createNewWorkflow, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        assertEquals("1", responseCaptor.getValue().getWorkflowId());

    }

    public void testFailedToCreateNewWorkflow() {
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest createNewWorkflow = new WorkflowRequest(null, template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to create global_context index"));
            return null;
        }).when(globalContextHandler).putTemplateToGlobalContext(any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), createNewWorkflow, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to create global_context index", exceptionCaptor.getValue().getMessage());
    }

    public void testUpdateWorkflow() {

        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new IndexResponse(new ShardId(GLOBAL_CONTEXT_INDEX, "", 1), "1", 1L, 1L, 1L, true));
            return null;
        }).when(globalContextHandler).updateTemplateInGlobalContext(any(), any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());

        assertEquals("1", responseCaptor.getValue().getWorkflowId());
    }

    public void testFailedToUpdateWorkflow() {
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest updateWorkflow = new WorkflowRequest("1", template);

        doAnswer(invocation -> {
            ActionListener<IndexResponse> responseListener = invocation.getArgument(2);
            responseListener.onFailure(new Exception("Failed to update use case template"));
            return null;
        }).when(globalContextHandler).updateTemplateInGlobalContext(any(), any(Template.class), any());

        createWorkflowTransportAction.doExecute(mock(Task.class), updateWorkflow, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to update use case template", exceptionCaptor.getValue().getMessage());
    }
}
