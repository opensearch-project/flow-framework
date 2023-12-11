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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.index.get.GetResult;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GetTemplateTransportActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Client client;
    private GetTemplateTransportAction getTemplateTransportAction;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private Template template;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = mock(ThreadPool.class);
        this.client = mock(Client.class);
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        this.getTemplateTransportAction = new GetTemplateTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            flowFrameworkIndicesHandler,
            client
        );

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
            Map.of("provision", workflow),
            Map.of(),
            TestHelpers.randomUser()
        );

        ThreadPool clientThreadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);

    }

    public void testGetTemplateNoGlobalContext() {

        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(false);
        @SuppressWarnings("unchecked")
        ActionListener<GetTemplateResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest("1", null);
        getTemplateTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("There are no templates in the global_context"));
    }

    public void testGetTemplateSuccess() {
        String workflowId = "12345";
        @SuppressWarnings("unchecked")
        ActionListener<GetTemplateResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(true);

        // Stub client.get to force on response
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            this.template.toXContent(builder, null);
            BytesReference templateBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, workflowId, 1, 1, 1, true, templateBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        getTemplateTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<GetTemplateResponse> templateCaptor = ArgumentCaptor.forClass(GetTemplateResponse.class);
        verify(listener, times(1)).onResponse(templateCaptor.capture());
        assertEquals(this.template.name(), templateCaptor.getValue().getTemplate().name());
    }

    public void testGetTemplateFailure() {
        String workflowId = "12345";
        @SuppressWarnings("unchecked")
        ActionListener<GetTemplateResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        when(flowFrameworkIndicesHandler.doesIndexExist(anyString())).thenReturn(true);

        // Stub client.get to force on failure
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to retrieve template from global context."));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        getTemplateTransportAction.doExecute(mock(Task.class), workflowRequest, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to retrieve template from global context.", exceptionCaptor.getValue().getMessage());
    }
}