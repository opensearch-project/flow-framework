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
import org.opensearch.ExceptionsHelper;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.transport.DeprovisionWorkflowAction;
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
 * Rest Action to facilitate requests to de-provision a workflow
 */
public class RestDeprovisionWorkflowAction extends BaseRestHandler {

    private static final String DEPROVISION_WORKFLOW_ACTION = "deprovision_workflow";
    private static final Logger logger = LogManager.getLogger(RestDeprovisionWorkflowAction.class);
    private final FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;

    /**
     * Instantiates a new RestDeprovisionWorkflowAction
     * @param flowFrameworkFeatureEnabledSetting Whether this API is enabled
     */
    public RestDeprovisionWorkflowAction(FlowFrameworkSettings flowFrameworkFeatureEnabledSetting) {
        this.flowFrameworkFeatureEnabledSetting = flowFrameworkFeatureEnabledSetting;
    }

    @Override
    public String getName() {
        return DEPROVISION_WORKFLOW_ACTION;
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
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
                throw new FlowFrameworkException("deprovision request should have no payload", RestStatus.BAD_REQUEST);
            }
            // Validate params
            if (workflowId == null) {
                throw new FlowFrameworkException("workflow_id cannot be null", RestStatus.BAD_REQUEST);
            }
            WorkflowRequest workflowRequest = new WorkflowRequest(workflowId, null);

            return channel -> client.execute(DeprovisionWorkflowAction.INSTANCE, workflowRequest, ActionListener.wrap(response -> {
                XContentBuilder builder = response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }, exception -> {
                try {
                    FlowFrameworkException ex;
                    if (exception instanceof WorkflowStepException) {
                        ex = (WorkflowStepException) exception;
                    } else if (exception instanceof FlowFrameworkException) {
                        ex = (FlowFrameworkException) exception;
                    } else {
                        ex = new FlowFrameworkException("Failed to deprovision workflow.", ExceptionsHelper.status(exception));
                    }
                    XContentBuilder exceptionBuilder = ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(ex.getRestStatus(), exceptionBuilder));
                } catch (IOException e) {
                    String errorMessage = "IOException: Failed to send back deprovision workflow exception";
                    logger.error(errorMessage, e);
                    channel.sendResponse(new BytesRestResponse(ExceptionsHelper.status(e), errorMessage));
                }
            }));
        } catch (FlowFrameworkException ex) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(ex.getRestStatus(), ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOW_URI, WORKFLOW_ID, "_deprovision"))
        );
    }
}
