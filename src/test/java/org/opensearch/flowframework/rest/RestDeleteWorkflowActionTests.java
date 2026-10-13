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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestDeleteWorkflowActionTests extends OpenSearchTestCase {
    private RestDeleteWorkflowAction restDeleteWorkflowAction;
    private String getPath;
    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.getPath = String.format(Locale.ROOT, "%s/{%s}", WORKFLOW_URI, "workflow_id");
        flowFrameworkFeatureEnabledSetting = mock(FlowFrameworkSettings.class);
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(true);
        this.restDeleteWorkflowAction = new RestDeleteWorkflowAction(flowFrameworkFeatureEnabledSetting);
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestDeleteWorkflowActionName() {
        String name = restDeleteWorkflowAction.getName();
        assertEquals("delete_workflow", name);
    }

    public void testRestDeleteWorkflowActionRoutes() {
        List<RestHandler.Route> routes = restDeleteWorkflowAction.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.DELETE, routes.get(0).getMethod());
        assertEquals(this.getPath, routes.get(0).getPath());
    }

    public void testNullWorkflowId() throws Exception {

        // Request with no params
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath(this.getPath)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        restDeleteWorkflowAction.handleRequest(request, channel, nodeClient);

        assertEquals(1, channel.errors().get());
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("workflow_id cannot be null"));
    }

    public void testFeatureFlagNotEnabled() throws Exception {
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(false);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath(this.getPath)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        restDeleteWorkflowAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.FORBIDDEN, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("This API is disabled."));
    }
}
