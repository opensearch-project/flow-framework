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
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
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

    public void testRestProvisiionWorkflowActionRoutes() {
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

    public void testInvalidRequestWithContent() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.provisionWorkflowPath)
            .withContent(new BytesArray("request body"), MediaTypeRegistry.JSON)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            provisionWorkflowRestAction.handleRequest(request, channel, nodeClient);
        });
        assertEquals(
            "request [POST /_plugins/_flow_framework/workflow/{workflow_id}/_provision] does not support having a body",
            ex.getMessage()
        );
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
}
