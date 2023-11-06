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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.GetWorkflowAction;
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
 * Rest Action to facilitate requests to get a workflow status
 */
public class RestGetWorkflowAction extends BaseRestHandler {

    private static final String GET_WORKFLOW_ACTION = "get_workflow";
    private static final Logger logger = LogManager.getLogger(RestGetWorkflowAction.class);

    /**
     * Instantiates a new RestGetWorkflowAction
     */
    public RestGetWorkflowAction() {}

    @Override
    public String getName() {
        return GET_WORKFLOW_ACTION;
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Validate content
        if (request.hasContent()) {
            throw new FlowFrameworkException("Invalid request format", RestStatus.BAD_REQUEST);
        }
        // Validate params
        String workflowId = request.param(WORKFLOW_ID);
        if (workflowId == null) {
            throw new FlowFrameworkException("workflow_id cannot be null", RestStatus.BAD_REQUEST);
        }

        String rawPath = request.rawPath();
        boolean all = request.paramAsBoolean("_all", false);
        // Create request and provision
        WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null, all);
        return channel -> client.execute(GetWorkflowAction.INSTANCE, workflowRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
            // Provision workflow from indexed use case template
            new Route(RestRequest.Method.GET, String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOW_URI, WORKFLOW_ID, "_status"))
        );
    }
}
