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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.transport.DeprovisionWorkflowAction;
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

import static org.opensearch.flowframework.common.CommonValue.ALLOW_DELETE;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RestDeprovisionWorkflowActionTests extends OpenSearchTestCase {

    private RestDeprovisionWorkflowAction deprovisionWorkflowRestAction;
    private String deprovisionWorkflowPath;
    private NodeClient nodeClient;
    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        flowFrameworkFeatureEnabledSetting = mock(FlowFrameworkSettings.class);
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(true);

        this.deprovisionWorkflowRestAction = spy(new RestDeprovisionWorkflowAction(flowFrameworkFeatureEnabledSetting));
        this.deprovisionWorkflowPath = String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOW_URI, "workflow_id", "_deprovision");
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestDeprovisionWorkflowActionName() {
        String name = deprovisionWorkflowRestAction.getName();
        assertEquals("deprovision_workflow", name);
    }

    public void testRestDeprovisiionWorkflowActionRoutes() {
        List<Route> routes = deprovisionWorkflowRestAction.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(this.deprovisionWorkflowPath, routes.get(0).getPath());
    }

    public void testNullWorkflowId() throws Exception {
        // Request with no params
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.deprovisionWorkflowPath)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        deprovisionWorkflowRestAction.handleRequest(request, channel, nodeClient);

        assertEquals(1, channel.errors().get());
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("workflow_id cannot be null"));
    }

    public void testAllowDeleteParam() throws Exception {
        String allowDeleteParam = "foo,bar";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.deprovisionWorkflowPath)
            .withParams(Map.ofEntries(Map.entry(WORKFLOW_ID, "workflow_id"), Map.entry(ALLOW_DELETE, allowDeleteParam)))
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        doAnswer(invocation -> {
            WorkflowRequest workflowRequest = invocation.getArgument(1);
            ActionListener<WorkflowResponse> responseListener = invocation.getArgument(2);
            responseListener.onResponse(new WorkflowResponse(workflowRequest.getParams().get(ALLOW_DELETE)));
            return null;
        }).when(nodeClient).execute(any(DeprovisionWorkflowAction.class), any(WorkflowRequest.class), any());

        deprovisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.OK, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains(allowDeleteParam));
    }

    public void testFeatureFlagNotEnabled() throws Exception {
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(false);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.deprovisionWorkflowPath)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        deprovisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(1, channel.errors().get());
        assertEquals(RestStatus.FORBIDDEN, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("This API is disabled."));
    }
}
