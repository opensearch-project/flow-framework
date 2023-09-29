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
import org.opensearch.flowframework.FlowFrameworkPlugin;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.transport.CreateWorkflowAction;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Rest Action to facilitate requests to create and update a use case template
 */
public class RestCreateWorkflowAction extends BaseRestHandler {

    private static final String CREATE_WORKFLOW_ACTION = "create_workflow_action";

    // TODO : move to common values class, pending implementation
    /**
     * Field name for workflow Id, the document Id of the indexed use case template
     */
    public static final String WORKFLOW_ID = "workflowId";

    /**
     * Intantiates a new RestCreateWorkflowAction
     */
    public RestCreateWorkflowAction() {
        // TODO : Pass settings and cluster service to constructor and add settings update consumer for request timeout value
    }

    @Override
    public String getName() {
        return CREATE_WORKFLOW_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            // Create new workflow
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s", FlowFrameworkPlugin.WORKFLOWS_URI)),
            // Update workflow
            new Route(RestRequest.Method.PUT, String.format(Locale.ROOT, "%s/{%s}", FlowFrameworkPlugin.WORKFLOWS_URI, WORKFLOW_ID))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String workflowId = request.param(WORKFLOW_ID, null);
        Template template = Template.parse(request.content().utf8ToString());
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, template);
        return channel -> client.execute(CreateWorkflowAction.INSTANCE, workflowRequest, new RestToXContentListener<>(channel));
    }

}
