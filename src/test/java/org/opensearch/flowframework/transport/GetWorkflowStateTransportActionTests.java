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
import org.opensearch.client.Client;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.Assert;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetWorkflowStateTransportActionTests extends OpenSearchTestCase {

    private GetWorkflowStateTransportAction getWorkflowStateTransportAction;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private Client client;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private ActionListener<GetWorkflowStateResponse> response;
    private Task task;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.client = mock(Client.class);
        this.threadPool = mock(ThreadPool.class);
        this.getWorkflowStateTransportAction = new GetWorkflowStateTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            xContentRegistry()
        );
        task = Mockito.mock(Task.class);
        ThreadPool clientThreadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(clientThreadPool);
        when(clientThreadPool.getThreadContext()).thenReturn(threadContext);

        response = new ActionListener<GetWorkflowStateResponse>() {
            @Override
            public void onResponse(GetWorkflowStateResponse getResponse) {
                assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {}
        };

    }

    public void testGetTransportAction() throws IOException {
        GetWorkflowStateRequest getWorkflowRequest = new GetWorkflowStateRequest("1234", false);
        getWorkflowStateTransportAction.doExecute(task, getWorkflowRequest, response);
    }

    public void testGetAction() {
        Assert.assertNotNull(GetWorkflowStateAction.INSTANCE.name());
        Assert.assertEquals(GetWorkflowStateAction.INSTANCE.name(), GetWorkflowStateAction.NAME);
    }

    public void testGetWorkflowStateRequest() throws IOException {
        GetWorkflowStateRequest request = new GetWorkflowStateRequest("1234", false);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        GetWorkflowStateRequest newRequest = new GetWorkflowStateRequest(input);
        Assert.assertEquals(request.getWorkflowId(), newRequest.getWorkflowId());
        Assert.assertEquals(request.getAll(), newRequest.getAll());
        Assert.assertNull(newRequest.validate());
    }

    public void testGetWorkflowStateResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        String workflowId = randomAlphaOfLength(5);
        WorkflowState workFlowState = new WorkflowState(
            workflowId,
            "test",
            "PROVISIONING",
            "IN_PROGRESS",
            Instant.now(),
            Instant.now(),
            TestHelpers.randomUser(),
            Collections.emptyMap(),
            Collections.emptyList()
        );

        GetWorkflowStateResponse response = new GetWorkflowStateResponse(workFlowState, false);
        response.writeTo(out);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry());
        GetWorkflowStateResponse newResponse = new GetWorkflowStateResponse(input);
        XContentBuilder builder = TestHelpers.builder();
        Assert.assertNotNull(newResponse.toXContent(builder, ToXContent.EMPTY_PARAMS));

        Map<String, Object> map = TestHelpers.XContentBuilderToMap(builder);
        Assert.assertEquals(map.get("state"), workFlowState.getState());
        Assert.assertEquals(map.get("workflow_id"), workFlowState.getWorkflowId());
    }
}
