/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.Version;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.DefaultUseCases;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.flowframework.transport.WorkflowResponse;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.CREATE_CONNECTOR_CREDENTIAL_KEY;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.USE_CASE;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestCreateWorkflowActionTests extends OpenSearchTestCase {

    private String validTemplate;
    private String invalidTemplate;
    private RestCreateWorkflowAction createWorkflowRestAction;
    private String createWorkflowPath;
    private String updateWorkflowPath;
    private NodeClient nodeClient;
    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        flowFrameworkFeatureEnabledSetting = mock(FlowFrameworkSettings.class);
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(true);

        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        WorkflowNode nodeA = new WorkflowNode("A", "a-type", Collections.emptyMap(), Map.of("foo", "bar"));
        WorkflowNode nodeB = new WorkflowNode("B", "b-type", Collections.emptyMap(), Map.of("baz", "qux"));
        WorkflowEdge edgeAB = new WorkflowEdge("A", "B");
        List<WorkflowNode> nodes = List.of(nodeA, nodeB);
        List<WorkflowEdge> edges = List.of(edgeAB);
        Workflow workflow = new Workflow(Map.of("key", "value"), nodes, edges);

        Template template = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null
        );

        // Invalid template configuration, wrong field name
        this.validTemplate = template.toJson();
        this.invalidTemplate = this.validTemplate.replace("use_case", "invalid");
        this.createWorkflowRestAction = new RestCreateWorkflowAction(flowFrameworkFeatureEnabledSetting);
        this.createWorkflowPath = String.format(Locale.ROOT, "%s", WORKFLOW_URI);
        this.updateWorkflowPath = String.format(Locale.ROOT, "%s/{%s}", WORKFLOW_URI, "workflow_id");
        this.nodeClient = mock(NodeClient.class);
    }

    public void testRestCreateWorkflowActionName() {
        String name = createWorkflowRestAction.getName();
        assertEquals("create_workflow_action", name);
    }

    public void testRestCreateWorkflowActionRoutes() {
        List<Route> routes = createWorkflowRestAction.routes();
        assertEquals(2, routes.size());
        assertEquals(RestRequest.Method.POST, routes.get(0).getMethod());
        assertEquals(RestRequest.Method.PUT, routes.get(1).getMethod());
        assertEquals(this.createWorkflowPath, routes.get(0).getPath());
        assertEquals(this.updateWorkflowPath, routes.get(1).getPath());

    }

    public void testCreateWorkflowRequestWithParamsAndProvision() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.createWorkflowPath)
            .withParams(Map.ofEntries(Map.entry(PROVISION_WORKFLOW, "true"), Map.entry("foo", "bar")))
            .withContent(new BytesArray(validTemplate), MediaTypeRegistry.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(new WorkflowResponse("id-123"));
            return null;
        }).when(nodeClient).execute(any(), any(WorkflowRequest.class), any());
        createWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.CREATED, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("id-123"));
    }

    public void testCreateWorkflowRequestWithParamsButNoProvision() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.createWorkflowPath)
            .withParams(Map.of("foo", "bar"))
            .withContent(new BytesArray(validTemplate), MediaTypeRegistry.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        createWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(
            channel.capturedResponse().content().utf8ToString().contains("are permitted unless the provision parameter is set to true.")
        );
    }

    public void testCreateWorkflowRequestWithUseCaseButNoProvision() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.createWorkflowPath)
            .withParams(Map.of(USE_CASE, DefaultUseCases.COHERE_EMBEDDING_MODEL_DEPLOY.getUseCaseName()))
            .withContent(new BytesArray(""), MediaTypeRegistry.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(new WorkflowResponse("id-123"));
            return null;
        }).when(nodeClient).execute(any(), any(WorkflowRequest.class), any());
        createWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.CREATED, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("id-123"));
    }

    public void testCreateWorkflowRequestWithUseCaseAndContent() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.createWorkflowPath)
            .withParams(Map.of(USE_CASE, DefaultUseCases.COHERE_EMBEDDING_MODEL_DEPLOY.getUseCaseName()))
            .withContent(new BytesArray("{\"" + CREATE_CONNECTOR_CREDENTIAL_KEY + "\":\"step\"}"), MediaTypeRegistry.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        doAnswer(invocation -> {
            ActionListener<WorkflowResponse> actionListener = invocation.getArgument(2);
            actionListener.onResponse(new WorkflowResponse("id-123"));
            return null;
        }).when(nodeClient).execute(any(), any(WorkflowRequest.class), any());
        createWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.CREATED, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("id-123"));
    }

    public void testInvalidCreateWorkflowRequest() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.createWorkflowPath)
            .withContent(new BytesArray(invalidTemplate), MediaTypeRegistry.JSON)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        createWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("Unable to parse field [invalid] in a template object."));
    }

    public void testFeatureFlagNotEnabled() throws Exception {
        when(flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()).thenReturn(false);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath(this.createWorkflowPath)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        createWorkflowRestAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.FORBIDDEN, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("This API is disabled."));
    }
}
