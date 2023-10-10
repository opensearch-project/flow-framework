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
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProvisionWorkflowTransportActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Client client;
    private WorkflowProcessSorter workflowProcessSorter;
    private ProvisionWorkflowTransportAction provisionWorkflowTransportAction;
    private Template template;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = mock(ThreadPool.class);
        this.client = mock(Client.class);
        this.workflowProcessSorter = mock(WorkflowProcessSorter.class);

        this.provisionWorkflowTransportAction = new ProvisionWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            threadPool,
            client,
            workflowProcessSorter
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
            Map.of("provision", workflow),
            Map.of("outputKey", "outputValue"),
            Map.of("resourceKey", "resourceValue")
        );

        ThreadPool clientThreadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);
    }

    public void testProvisionWorkflow() {

        String workflowId = "1";
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            this.template.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference templateBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, workflowId, 1, 1, 1, true, templateBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(), any());

        provisionWorkflowTransportAction.doExecute(mock(Task.class), workflowRequest, listener);
        ArgumentCaptor<WorkflowResponse> responseCaptor = ArgumentCaptor.forClass(WorkflowResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(workflowId, responseCaptor.getValue().getWorkflowId());
    }

    public void testFailedToRetrieveTemplateFromGlobalContext() {
        ActionListener<WorkflowResponse> listener = mock(ActionListener.class);
        WorkflowRequest request = new WorkflowRequest("1", null);
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to retrieve template from global context."));
            return null;
        }).when(client).get(any(), any());

        provisionWorkflowTransportAction.doExecute(mock(Task.class), request, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to retrieve template from global context.", exceptionCaptor.getValue().getMessage());
    }

}
