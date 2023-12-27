/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.common.FlowFrameworkFeatureEnabledSetting;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.ProvisionWorkflowAction;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;

/**
 * Rest action to facilitate requests to provision a workflow from an inline defined or stored use case template
 */
public class RestProvisionWorkflowAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestProvisionWorkflowAction.class);

    private static final String PROVISION_WORKFLOW_ACTION = "provision_workflow_action";

    private FlowFrameworkFeatureEnabledSetting flowFrameworkFeatureEnabledSetting;

    /**
     * Instantiates a new RestProvisionWorkflowAction
     *
     * @param flowFrameworkFeatureEnabledSetting Whether this API is enabled
     */
    public RestProvisionWorkflowAction(FlowFrameworkFeatureEnabledSetting flowFrameworkFeatureEnabledSetting) {
        this.flowFrameworkFeatureEnabledSetting = flowFrameworkFeatureEnabledSetting;
    }

    @Override
    public String getName() {
        return PROVISION_WORKFLOW_ACTION;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            // Provision workflow from indexed use case template
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOW_URI, WORKFLOW_ID, "_provision"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String workflowId = request.param(WORKFLOW_ID);
        try {
            if (!flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()) {
                throw new FlowFrameworkException(
                    "This API is disabled. To enable it, update the setting [" + FLOW_FRAMEWORK_ENABLED.getKey() + "] to true.",
                    RestStatus.FORBIDDEN
                );
            }
            // Validate content
            if (request.hasContent()) {
                // BaseRestHandler will give appropriate error message
                return channel -> channel.sendResponse(null);
            }
            // Validate params
            if (workflowId == null) {
                throw new FlowFrameworkException("workflow_id cannot be null", RestStatus.BAD_REQUEST);
            }
            // Create request and provision
            WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);
            return channel -> client.execute(ProvisionWorkflowAction.INSTANCE, workflowRequest, ActionListener.wrap(response -> {
                XContentBuilder builder = response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }, exception -> {
                try {
                    FlowFrameworkException ex = (FlowFrameworkException) exception;
                    XContentBuilder exceptionBuilder = ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(ex.getRestStatus(), exceptionBuilder));
                } catch (IOException e) {
                    logger.error("Failed to send back provision workflow exception", e);
                }
            }));
        } catch (FlowFrameworkException ex) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(ex.getRestStatus(), ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
    }

}
