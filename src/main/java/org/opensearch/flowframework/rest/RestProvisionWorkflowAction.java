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
import org.opensearch.flowframework.transport.ProvisionWorkflowAction;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * Rest action to facilitate requests to provision a workflow from an inline defined or stored use case template
 */
public class RestProvisionWorkflowAction extends BaseRestHandler {

    private static final String PROVISION_WORKFLOW_ACTION = "provision_workflow_action";

    // TODO : move to common values class, pending implementation
    /**
     * Field name for workflow Id, the document Id of the indexed use case template
     */
    public static final String WORKFLOW_ID = "workflowId";

    /**
     * Instantiates a new RestProvisionWorkflowAction
     */
    public RestProvisionWorkflowAction() {
        // TODO : Pass settings and cluster service to constructor and add settings update consumer for request timeout value
    }

    @Override
    public String getName() {
        return PROVISION_WORKFLOW_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            // Provision workflow from inline use case template
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/%s", FlowFrameworkPlugin.WORKFLOWS_URI, "_provision")),
            // Provision workflow from indexed use case template
            new Route(
                RestRequest.Method.POST,
                String.format(Locale.ROOT, "%s/{%s}/%s", FlowFrameworkPlugin.WORKFLOWS_URI, WORKFLOW_ID, "_provision")
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        String workflowId = request.param(WORKFLOW_ID, null);
        Template template = null;

        if (request.hasContent()) {
            template = Template.parse(request.content().utf8ToString());
        }

        // Validate workflow request inputs
        if (workflowId == null && template == null) {
            throw new IOException("WorkflowId and template cannot be both null");
        }

        // Create request and provision
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, template);
        return channel -> client.execute(ProvisionWorkflowAction.INSTANCE, workflowRequest, new RestToXContentListener<>(channel));
    }

}
