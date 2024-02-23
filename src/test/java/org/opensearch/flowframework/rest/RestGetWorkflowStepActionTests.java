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
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.flowframework.transport.GetWorkflowStepResponse;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestGetWorkflowStepActionTests extends OpenSearchTestCase {
    private RestGetWorkflowStepAction restGetWorkflowStepAction;
    private String getPath;
    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;
    private NodeClient nodeClient;
    private WorkflowStepFactory workflowStepFactory;
    private FlowFrameworkSettings flowFrameworkSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);

        this.getPath = String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, "_steps");
        ThreadPool threadPool = mock(ThreadPool.class);
        MachineLearningNodeClient mlClient = mock(MachineLearningNodeClient.class);
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        this.workflowStepFactory = new WorkflowStepFactory(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings);
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
        assertEquals("request [GET /_plugins/_flow_framework/workflow/_steps] does not support having a body", ex.getMessage());
    }

    public void testWorkflowSteps() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(this.getPath + "?workflow_step=create_connector")
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        List<String> steps = new ArrayList<>();
        steps.add("create_connector");
        doAnswer(invocation -> {
            ActionListener<GetWorkflowStepResponse> actionListener = invocation.getArgument(2);
            WorkflowValidator workflowValidator = this.workflowStepFactory.getWorkflowValidatorByStep(steps);
            actionListener.onResponse(new GetWorkflowStepResponse(workflowValidator));
            return null;
        }).when(nodeClient).execute(any(), any(WorkflowRequest.class), any());
        restGetWorkflowStepAction.handleRequest(request, channel, nodeClient);
        assertEquals(RestStatus.OK, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("create_connector"));
    }

    public void testFailedWorkflowSteps() throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(this.getPath + "?step=xyz")
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        List<String> steps = new ArrayList<>();
        steps.add("xyz");
        doAnswer(invocation -> {
            ActionListener<GetWorkflowStepResponse> actionListener = invocation.getArgument(2);
            WorkflowValidator workflowValidator = this.workflowStepFactory.getWorkflowValidatorByStep(steps);
            actionListener.onResponse(new GetWorkflowStepResponse(workflowValidator));
            return null;
        }).when(nodeClient).execute(any(), any(WorkflowRequest.class), any());
        FlowFrameworkException exception = expectThrows(
            FlowFrameworkException.class,
            () -> restGetWorkflowStepAction.handleRequest(request, channel, nodeClient)
        );
        assertEquals("Please only use only valid step name", exception.getMessage());
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
