/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.DynamoDbUtil.DDBClient;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;

/**
 * Transport action to retrieve a use case template within the Global Context
 */
public class GetWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, GetWorkflowResponse> {

    private final Logger logger = LogManager.getLogger(GetWorkflowTransportAction.class);

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final Client client;
    private final DDBClient ddbClient;
    private final EncryptorUtils encryptorUtils;

    /**
     * Instantiates a new GetWorkflowTransportAction instance
     * @param transportService the transport service
     * @param actionFilters action filters
     * @param flowFrameworkIndicesHandler The Flow Framework indices handler
     * @param encryptorUtils Encryptor utils
     * @param client the Opensearch Client
     * @param ddbClient The Dynamo DB client
     */
    @Inject
    public GetWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        Client client,
        DDBClient ddbClient,
        EncryptorUtils encryptorUtils
    ) {
        super(GetWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.client = client;
        this.ddbClient = ddbClient;
        this.encryptorUtils = encryptorUtils;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<GetWorkflowResponse> listener) {
        if (flowFrameworkIndicesHandler.doesIndexExist(GLOBAL_CONTEXT_INDEX)) {

            String workflowId = request.getWorkflowId();
            GetRequest getRequest = new GetRequest(GLOBAL_CONTEXT_INDEX, workflowId);

            // Retrieve workflow by ID
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                logger.info("Querying workflow from global context: {}", workflowId);
                ddbClient.get(getRequest, ActionListener.wrap(response -> {
                    context.restore();

                    if (!response.isExists()) {
                        String errorMessage = "Failed to retrieve template (" + workflowId + ") from global context.";
                        logger.error(errorMessage);
                        listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
                    } else {
                        // Remove any credential from response
                        Template template = encryptorUtils.redactTemplateCredentials(Template.parse(response.getSourceAsString()));
                        listener.onResponse(new GetWorkflowResponse(template));
                    }
                }, exception -> {
                    String errorMessage = "Failed to retrieve template (" + workflowId + ") from global context.";
                    logger.error(errorMessage, exception);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }));
            } catch (Exception e) {
                String errorMessage = "Failed to retrieve template (" + workflowId + ") from global context.";
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        } else {
            String errorMessage = "There are no templates in the global_context";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        }

    }
}
