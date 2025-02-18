/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestSearchWorkflowStateActionTests extends OpenSearchTestCase {
    private RestSearchWorkflowStateAction restSearchWorkflowStateAction;
    private String searchPath;
    private NodeClient nodeClient;
    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.searchPath = String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, "state/_search");
        flowFrameworkFeatureEnabledSetting = mock(FlowFrameworkSettings.class);
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(true);
        this.restSearchWorkflowStateAction = new RestSearchWorkflowStateAction(flowFrameworkFeatureEnabledSetting);
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestSearchWorkflowStateActionName() {
        String name = restSearchWorkflowStateAction.getName();
        assertEquals("search_workflow_state_action", name);
    }

    public void testRestSearchWorkflowStateActionRoutes() {
        List<Route> routes = restSearchWorkflowStateAction.routes();
        assertNotNull(routes);
        assertEquals(2, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(RestRequest.Method.GET, routes.get(1).getMethod());
        assertEquals(this.searchPath, routes.get(0).getPath());
        assertEquals(this.searchPath, routes.get(1).getPath());
    }

    public void testInvalidSearchRequest() {
        final String requestContent = "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"template\":\"1.0.0\"}}]}}}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(this.searchPath)
            .withContent(new BytesArray(requestContent), MediaTypeRegistry.JSON)
            .build();

        XContentParseException ex = expectThrows(XContentParseException.class, () -> {
            restSearchWorkflowStateAction.prepareRequest(request, nodeClient);
        });
        assertEquals("unknown named object category [org.opensearch.index.query.QueryBuilder]", ex.getMessage());
    }

    public void testFeatureFlagNotEnabled() throws Exception {
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(false);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.searchPath)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        restSearchWorkflowStateAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.FORBIDDEN, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("This API is disabled."));
    }
}
