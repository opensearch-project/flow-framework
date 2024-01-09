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

public class RestGetWorkflowStepActionTests extends OpenSearchTestCase {
    private RestGetWorkflowStepAction restGetWorkflowStepAction;
    private String getPath;
    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;
    private NodeClient nodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.getPath = String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, "_step");
        flowFrameworkFeatureEnabledSetting = mock(FlowFrameworkSettings.class);
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(true);
        this.restGetWorkflowStepAction = new RestGetWorkflowStepAction(flowFrameworkFeatureEnabledSetting);
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestGetWorkflowStepActionName() {
        String name = restGetWorkflowStepAction.getName();
        assertEquals("get_workflow_step", name);
    }

    public void testRestGetWorkflowStepActionRoutes() {
        List<RestHandler.Route> routes = restGetWorkflowStepAction.routes();
        assertEquals(1, routes.size());
        assertEquals(RestRequest.Method.GET, routes.get(0).getMethod());
        assertEquals(this.getPath, routes.get(0).getPath());
    }

    public void testInvalidRequestWithContent() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(this.getPath)
            .withContent(new BytesArray("request body"), MediaTypeRegistry.JSON)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
            restGetWorkflowStepAction.handleRequest(request, channel, nodeClient);
        });
        assertEquals("request [GET /_plugins/_flow_framework/workflow/_step] does not support having a body", ex.getMessage());
    }

    public void testFeatureFlagNotEnabled() throws Exception {
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(false);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath(this.getPath)
                .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        restGetWorkflowStepAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.FORBIDDEN, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("This API is disabled."));
    }
}
