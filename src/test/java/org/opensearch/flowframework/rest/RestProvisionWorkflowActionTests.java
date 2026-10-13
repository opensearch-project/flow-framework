/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.flowframework.transport.WorkflowResponse;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestProvisionWorkflowActionTests extends OpenSearchTestCase {

    private RestProvisionWorkflowAction provisionWorkflowRestAction;
    private String provisionWorkflowPath;
    private NodeClient nodeClient;
    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        flowFrameworkFeatureEnabledSetting = mock(FlowFrameworkSettings.class);
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(true);

        this.provisionWorkflowRestAction = new RestProvisionWorkflowAction(flowFrameworkFeatureEnabledSetting);
        this.provisionWorkflowPath = String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOW_URI, "workflow_id", "_provision");
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestProvisionWorkflowActionName() {
        String name = provisionWorkflowRestAction.getName();
        assertEquals("provision_workflow_action", name);
    }

    public void testRestProvisionWorkflowActionRoutes() {
        List<Route> routes = provisionWorkflowRestAction.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(this.provisionWorkflowPath, routes.get(0).getPath());
    }

    public void testNullWorkflowId() throws Exception {

        // Request with no params
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);

        assertEquals(1, channel.errors().get());
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("workflow_id cannot be null"));
    }

    public void testContentParsing() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .withParams(Map.of("workflow_id", "abc"))
            .withContent(new BytesArray("{\"foo\": \"bar\"}"), MediaTypeRegistry.JSON)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(new WorkflowResponse("id-123"));
            return null;
        }).when(nodeClient).execute(any(), any(WorkflowRequest.class), any());
        provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.OK, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("id-123"));
    }

    public void testContentParsingDuplicate() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .withParams(Map.ofEntries(Map.entry("workflow_id", "abc"), Map.entry("foo", "bar")))
            .withContent(new BytesArray("{\"bar\": \"none\", \"foo\": \"baz\"}"), MediaTypeRegistry.JSON)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        // assertEquals("", channel.capturedResponse().content().utf8ToString());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("Duplicate key foo"));
    }

    public void testContentParsingBadType() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .withParams(Map.of("workflow_id", "abc"))
            .withContent(new BytesArray("{\"foo\": 123}"), MediaTypeRegistry.JSON)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("Request body fields must have string values"));
    }

    public void testContentParsingError() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .withContent(new BytesArray("not json"), MediaTypeRegistry.JSON)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("Request body parsing failed"));
    }

    public void testFeatureFlagNotEnabled() throws Exception {
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(false);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.FORBIDDEN, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("This API is disabled."));
    }

    public void testProvisionWorkflowWithValidWaitForCompletionTimeout() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .withParams(Map.of("workflow_id", "abc", "wait_for_completion_timeout", "5s"))
            .withContent(new BytesArray("{\"foo\": \"bar\"}"), MediaTypeRegistry.JSON)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);

        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(new WorkflowResponse("workflow_1"));
            return null;
        }).when(nodeClient).execute(any(), any(WorkflowRequest.class), any());

        provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);

        assertEquals(RestStatus.OK, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("workflow_1"));
    }

}
