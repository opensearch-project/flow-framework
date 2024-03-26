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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.OrchestrateAction;
import org.opensearch.flowframework.transport.OrchestrateRequest;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

/**
 * Rest action to orchestate workflows
 */
public class RestOrchestrateAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestProvisionWorkflowAction.class);

    private static final String ORCHESTRATE_ACTION = "orchestrate_action";

    /*
     * Creates a new RestOrchestrateAction
     */
    public RestOrchestrateAction() {}

    @Override
    public String getName() {
        return ORCHESTRATE_ACTION;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/{%s}/%s", WORKFLOW_URI, WORKFLOW_ID, "_orchestrate"))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {

        // Get workflow ID
        String workflowId = request.param(WORKFLOW_ID);

        try {

            // Validate params
            if (workflowId == null) {
                throw new FlowFrameworkException("workflow_id cannot be null", RestStatus.BAD_REQUEST);
            }

            // Retrieve string to string map from content
            if (request.hasContent()) {
                XContentParser parser = request.contentParser();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Map<String, String> userInputs = ParseUtils.parseStringToStringMap(parser);

                // Create Request object and execute transport action with client to pass ID and values
                OrchestrateRequest orchestrateRequest = new OrchestrateRequest(workflowId, userInputs);
                return channel -> client.execute(OrchestrateAction.INSTANCE, orchestrateRequest, ActionListener.wrap(response -> {
                    XContentBuilder builder = response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }, exception -> {
                    try {
                        FlowFrameworkException ex = exception instanceof FlowFrameworkException
                            ? (FlowFrameworkException) exception
                            : new FlowFrameworkException("Failed to get workflow.", ExceptionsHelper.status(exception));
                        XContentBuilder exceptionBuilder = ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS);
                        channel.sendResponse(new BytesRestResponse(ex.getRestStatus(), exceptionBuilder));
                    } catch (IOException e) {
                        String errorMessage = "IOException: Failed to send back orchestrate exception";
                        logger.error(errorMessage, e);
                        channel.sendResponse(new BytesRestResponse(ExceptionsHelper.status(e), errorMessage));
                    }
                }));
            } else {
                throw new FlowFrameworkException("Request has no content", RestStatus.BAD_REQUEST);
            }

        } catch (FlowFrameworkException ex) {
            return channel -> channel.sendResponse(
                new BytesRestResponse(ex.getRestStatus(), ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }

    }
}
