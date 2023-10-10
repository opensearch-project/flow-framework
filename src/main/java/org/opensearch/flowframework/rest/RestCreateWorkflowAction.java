/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import com.google.common.collect.ImmutableList;
import org.opensearch.client.node.NodeClient;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.transport.CreateWorkflowAction;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

/**
 * Rest Action to facilitate requests to create and update a use case template
 */
public class RestCreateWorkflowAction extends BaseRestHandler {

    private static final String CREATE_WORKFLOW_ACTION = "create_workflow_action";

    /**
     * Intantiates a new RestCreateWorkflowAction
     */
    public RestCreateWorkflowAction() {}

    @Override
    public String getName() {
        return CREATE_WORKFLOW_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            // Create new workflow
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s", WORKFLOW_URI)),
            // Update use case template
            new Route(RestRequest.Method.PUT, String.format(Locale.ROOT, "%s/{%s}", WORKFLOW_URI, WORKFLOW_ID))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String workflowId = request.param(WORKFLOW_ID);
        Template template = Template.parse(request.content().utf8ToString());
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, template);
        return channel -> client.execute(CreateWorkflowAction.INSTANCE, workflowRequest, new RestToXContentListener<>(channel));
    }

}
