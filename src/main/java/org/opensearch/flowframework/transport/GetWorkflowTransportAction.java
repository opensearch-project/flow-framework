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
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;
import static org.opensearch.flowframework.util.ParseUtils.resolveUserAndExecute;

/**
 * Transport action to retrieve a use case template within the Global Context
 */
public class GetWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, GetWorkflowResponse> {

    private final Logger logger = LogManager.getLogger(GetWorkflowTransportAction.class);

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final Client client;
    private final EncryptorUtils encryptorUtils;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new GetWorkflowTransportAction instance
     * @param transportService the transport service
     * @param actionFilters action filters
     * @param flowFrameworkIndicesHandler The Flow Framework indices handler
     * @param encryptorUtils Encryptor utils
     * @param client the Opensearch Client
     * @param xContentRegistry contentRegister to parse get response
     * @param clusterService the cluster service
     * @param settings the plugin settings
     */
    @Inject
    public GetWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        Client client,
        EncryptorUtils encryptorUtils,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings
    ) {
        super(GetWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.client = client;
        this.encryptorUtils = encryptorUtils;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<GetWorkflowResponse> listener) {
        if (flowFrameworkIndicesHandler.doesIndexExist(GLOBAL_CONTEXT_INDEX)) {

            String workflowId = request.getWorkflowId();

            User user = getUserContext(client);

            // Retrieve workflow by ID
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {

                resolveUserAndExecute(
                    user,
                    workflowId,
                    filterByEnabled,
                    listener,
                    () -> executeGetRequest(request, listener, context),
                    client,
                    clusterService,
                    xContentRegistry
                );
            } catch (Exception e) {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Failed to retrieve template ({}) from global context.",
                    workflowId
                ).getFormattedMessage();
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        } else {
            String errorMessage = "There are no templates in the global_context";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        }

    }

    /**
     * Execute the get request against the global context index
     * @param request the workflow request
     * @param listener the action listener
     * @param context the thread context
     */
    private void executeGetRequest(
        WorkflowRequest request,
        ActionListener<GetWorkflowResponse> listener,
        ThreadContext.StoredContext context
    ) {
        String workflowId = request.getWorkflowId();
        GetRequest getRequest = new GetRequest(GLOBAL_CONTEXT_INDEX, workflowId);
        logger.info("Querying workflow from global context: {}", workflowId);
        client.get(getRequest, ActionListener.wrap(response -> {
            context.restore();

            if (!response.isExists()) {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Failed to retrieve template ({}) from global context.",
                    workflowId
                ).getFormattedMessage();
                logger.error(errorMessage);
                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
            } else {
                // Remove any secured field from response
                User user = ParseUtils.getUserContext(client);
                Template template = encryptorUtils.redactTemplateSecuredFields(user, Template.parse(response.getSourceAsString()));
                listener.onResponse(new GetWorkflowResponse(template));
            }
        }, exception -> {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to retrieve template ({}) from global context.",
                workflowId
            ).getFormattedMessage();
            logger.error(errorMessage, exception);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
        }));
    }
}
