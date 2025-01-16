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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.ProvisionWorkflowAction;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.flowframework.util.RestActionUtils;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.WAIT_FOR_COMPLETION_TIMEOUT;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.model.Template.createEmptyTemplateWithTenantId;

/**
 * Rest action to facilitate requests to provision a workflow from an inline defined or stored use case template
 */
public class RestProvisionWorkflowAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestProvisionWorkflowAction.class);

    private static final String PROVISION_WORKFLOW_ACTION = "provision_workflow_action";

    private FlowFrameworkSettings flowFrameworkFeatureEnabledSetting;

    /**
     * Instantiates a new RestProvisionWorkflowAction
     *
     * @param flowFrameworkFeatureEnabledSetting Whether this API is enabled
     */
    public RestProvisionWorkflowAction(FlowFrameworkSettings flowFrameworkFeatureEnabledSetting) {
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
        TimeValue waitForCompletionTimeout = request.paramAsTime(WAIT_FOR_COMPLETION_TIMEOUT, TimeValue.MINUS_ONE);
        try {
            Map<String, String> params = parseParamsAndContent(request);
            if (!flowFrameworkFeatureEnabledSetting.isFlowFrameworkEnabled()) {
                throw new FlowFrameworkException(
                    "This API is disabled. To enable it, update the setting [" + FLOW_FRAMEWORK_ENABLED.getKey() + "] to true.",
                    RestStatus.FORBIDDEN
                );
            }
            String tenantId = RestActionUtils.getTenantID(flowFrameworkFeatureEnabledSetting.isMultiTenancyEnabled(), request);
            // Validate params
            if (workflowId == null) {
                throw new FlowFrameworkException("workflow_id cannot be null", RestStatus.BAD_REQUEST);
            }
            // Create request and provision
            WorkflowRequest workflowRequest = new WorkflowRequest(
                workflowId,
                createEmptyTemplateWithTenantId(tenantId),
                params,
                waitForCompletionTimeout
            );
            return channel -> client.execute(ProvisionWorkflowAction.INSTANCE, workflowRequest, ActionListener.wrap(response -> {
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
                    String errorMessage = "IOException: Failed to send back provision workflow exception";
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

    private Map<String, String> parseParamsAndContent(RestRequest request) {
        // Get any other params from path
        Map<String, String> params = request.params()
            .keySet()
            .stream()
            .filter(k -> !WORKFLOW_ID.equals(k))
            .collect(Collectors.toMap(Function.identity(), request::param));
        // If body is included get any params from body
        if (request.hasContent()) {
            try (XContentParser parser = request.contentParser()) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String key = parser.currentName();
                    if (params.containsKey(key)) {
                        throw new FlowFrameworkException("Duplicate key " + key, RestStatus.BAD_REQUEST);
                    }
                    if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                        throw new FlowFrameworkException("Request body fields must have string values", RestStatus.BAD_REQUEST);
                    }
                    params.put(key, parser.text());
                }
            } catch (IOException e) {
                throw new FlowFrameworkException("Request body parsing failed", RestStatus.BAD_REQUEST);
            }
        }
        return params;
    }
}
