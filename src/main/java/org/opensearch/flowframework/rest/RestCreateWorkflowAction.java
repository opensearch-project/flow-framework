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
import org.opensearch.flowframework.common.DefaultUseCases;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.transport.CreateWorkflowAction;
import org.opensearch.flowframework.transport.WorkflowRequest;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.USE_CASE;
import static org.opensearch.flowframework.common.CommonValue.VALIDATION;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;

/**
 * Rest Action to facilitate requests to create and update a use case template
 */
public class RestCreateWorkflowAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RestCreateWorkflowAction.class);
    private static final String CREATE_WORKFLOW_ACTION = "create_workflow_action";

    private FlowFrameworkSettings flowFrameworkSettings;

    /**
     * Instantiates a new RestCreateWorkflowAction
     * @param flowFrameworkSettings The settings for the flow framework plugin
     */
    public RestCreateWorkflowAction(FlowFrameworkSettings flowFrameworkSettings) {
        this.flowFrameworkSettings = flowFrameworkSettings;
    }

    @Override
    public String getName() {
        return CREATE_WORKFLOW_ACTION;
    }

    @Override
    public List<Route> routes() {
        return List.of(
            // Create new workflow
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s", WORKFLOW_URI)),
            // Update use case template
            new Route(RestRequest.Method.PUT, String.format(Locale.ROOT, "%s/{%s}", WORKFLOW_URI, WORKFLOW_ID))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String workflowId = request.param(WORKFLOW_ID);
        String[] validation = request.paramAsStringArray(VALIDATION, new String[] { "all" });
        boolean provision = request.paramAsBoolean(PROVISION_WORKFLOW, false);
        String useCase = request.param(USE_CASE);
        // If provisioning, consume all other params and pass to provision transport action
        Map<String, String> params = provision
            ? request.params()
                .keySet()
                .stream()
                .filter(k -> !request.consumedParams().contains(k))
                .collect(Collectors.toMap(Function.identity(), request::param))
            : request.params()
                .entrySet()
                .stream()
                .filter(e -> !request.consumedParams().contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!flowFrameworkSettings.isFlowFrameworkEnabled()) {
            FlowFrameworkException ffe = new FlowFrameworkException(
                "This API is disabled. To enable it, set [" + FLOW_FRAMEWORK_ENABLED.getKey() + "] to true.",
                RestStatus.FORBIDDEN
            );
            return channel -> channel.sendResponse(
                new BytesRestResponse(ffe.getRestStatus(), ffe.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
        if (!provision && !params.isEmpty()) {
            // Consume params and content so custom exception is processed
            params.keySet().stream().forEach(request::param);
            request.content();
            FlowFrameworkException ffe = new FlowFrameworkException(
                "Only the parameters " + request.consumedParams() + " are permitted unless the provision parameter is set to true.",
                RestStatus.BAD_REQUEST
            );
            return channel -> channel.sendResponse(
                new BytesRestResponse(ffe.getRestStatus(), ffe.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
        try {

            Template template;
            Map<String, String> useCaseDefaultsMap = Collections.emptyMap();
            if (useCase != null) {
                String useCaseTemplateFileInStringFormat = ParseUtils.resourceToString(
                    "/" + DefaultUseCases.getSubstitutionReadyFileByUseCaseName(useCase)
                );
                String defaultsFilePath = DefaultUseCases.getDefaultsFileByUseCaseName(useCase);
                useCaseDefaultsMap = ParseUtils.parseJsonFileToStringToStringMap("/" + defaultsFilePath);

                if (request.hasContent()) {
                    try {
                        XContentParser parser = request.contentParser();
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        Map<String, Object> userDefaults = ParseUtils.parseStringToObjectMap(parser);
                        // updates the default params with anything user has given that matches
                        for (Map.Entry<String, Object> userDefaultsEntry : userDefaults.entrySet()) {
                            String key = userDefaultsEntry.getKey();
                            String value = userDefaultsEntry.getValue().toString();
                            if (useCaseDefaultsMap.containsKey(key)) {
                                useCaseDefaultsMap.put(key, value);
                            }
                        }
                    } catch (Exception ex) {
                        RestStatus status = ex instanceof IOException ? RestStatus.BAD_REQUEST : ExceptionsHelper.status(ex);
                        String errorMessage =
                            "failure parsing request body when a use case is given, make sure to provide a map with values that are either Strings, Arrays, or Map of Strings to Strings";
                        logger.error(errorMessage, ex);
                        throw new FlowFrameworkException(errorMessage, status);
                    }

                }

                useCaseTemplateFileInStringFormat = (String) ParseUtils.conditionallySubstitute(
                    useCaseTemplateFileInStringFormat,
                    null,
                    useCaseDefaultsMap
                );
                XContentParser parserTestJson = ParseUtils.jsonToParser(useCaseTemplateFileInStringFormat);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parserTestJson.currentToken(), parserTestJson);
                template = Template.parse(parserTestJson);

            } else {
                XContentParser parser = request.contentParser();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                template = Template.parse(parser);
            }

            WorkflowRequest workflowRequest = new WorkflowRequest(
                workflowId,
                template,
                validation,
                provision,
                params,
                useCase,
                useCaseDefaultsMap
            );

            return channel -> client.execute(CreateWorkflowAction.INSTANCE, workflowRequest, ActionListener.wrap(response -> {
                XContentBuilder builder = response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS);
                channel.sendResponse(new BytesRestResponse(RestStatus.CREATED, builder));
            }, exception -> {
                try {
                    FlowFrameworkException ex = exception instanceof FlowFrameworkException
                        ? (FlowFrameworkException) exception
                        : new FlowFrameworkException("Failed to get workflow.", ExceptionsHelper.status(exception));
                    XContentBuilder exceptionBuilder = ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(ex.getRestStatus(), exceptionBuilder));
                } catch (IOException e) {
                    String errorMessage = "IOException: Failed to send back create workflow exception";
                    logger.error(errorMessage, e);
                    channel.sendResponse(new BytesRestResponse(ExceptionsHelper.status(e), errorMessage));
                }
            }));

        } catch (FlowFrameworkException e) {
            logger.error("failed to prepare rest request", e);
            return channel -> channel.sendResponse(
                new BytesRestResponse(e.getRestStatus(), e.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        } catch (Exception e) {
            logger.error("failed to prepare rest request", e);
            FlowFrameworkException ex = new FlowFrameworkException(
                "IOException: template content invalid for specified Content-Type.",
                RestStatus.BAD_REQUEST
            );
            return channel -> channel.sendResponse(
                new BytesRestResponse(ex.getRestStatus(), ex.toXContent(channel.newErrorBuilder(), ToXContent.EMPTY_PARAMS))
            );
        }
    }
}
