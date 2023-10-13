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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.ProvisionWorkflowAction;
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
 * Rest action to facilitate requests to provision a workflow from an inline defined or stored use case template
 */
public class RestProvisionWorkflowAction extends BaseRestHandler {

    private static final String PROVISION_WORKFLOW_ACTION = "provision_workflow_action";

    /**
     * Instantiates a new RestProvisionWorkflowAction
     */
    public RestProvisionWorkflowAction() {}

    @Override
    public String getName() {
        return PROVISION_WORKFLOW_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            // Provision workflow from indexed use case template
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOW_URI, WORKFLOW_ID, "_provision"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // Validate content
        if (request.hasContent()) {
            throw new FlowFrameworkException("Invalid request format", RestStatus.BAD_REQUEST);
        }

        // Validate params
        String workflowId = request.param(WORKFLOW_ID);
        if (workflowId == null) {
            throw new FlowFrameworkException("workflow_id cannot be null", RestStatus.BAD_REQUEST);
        }

        // Create request and provision
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);
        return channel -> client.execute(ProvisionWorkflowAction.INSTANCE, workflowRequest, new RestToXContentListener<>(channel));
    }

}
