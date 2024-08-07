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
import org.apache.logging.log4j.core.util.Booleans;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
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
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.flowframework.common.CommonValue.CLEAR_STATUS;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;
import static org.opensearch.flowframework.util.ParseUtils.resolveUserAndExecute;

/**
 * Transport action to retrieve a use case template within the Global Context
 */
public class DeleteWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, DeleteResponse> {

    private final Logger logger = LogManager.getLogger(DeleteWorkflowTransportAction.class);

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final Client client;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new DeleteWorkflowTransportAction instance
     * @param transportService the transport service
     * @param actionFilters action filters
     * @param flowFrameworkIndicesHandler The Flow Framework indices handler
     * @param client the OpenSearch Client
     */
    @Inject
    public DeleteWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings
    ) {
        super(DeleteWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.client = client;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<DeleteResponse> listener) {
        if (flowFrameworkIndicesHandler.doesIndexExist(GLOBAL_CONTEXT_INDEX)) {
            String workflowId = request.getWorkflowId();
            User user = getUserContext(client);

            ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext();

            resolveUserAndExecute(
                user,
                workflowId,
                filterByEnabled,
                listener,
                () -> executeDeleteRequest(request, listener, context),
                client,
                clusterService,
                xContentRegistry
            );

        } else {
            String errorMessage = "There are no templates in the global context";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        }
    }

    protected void executeDeleteRequest(
        WorkflowRequest request,
        ActionListener<DeleteResponse> listener,
        ThreadContext.StoredContext context
    ) {
        String workflowId = request.getWorkflowId();
        DeleteRequest deleteRequest = new DeleteRequest(GLOBAL_CONTEXT_INDEX, workflowId);
        logger.info("Deleting workflow doc: {}", workflowId);
        client.delete(deleteRequest, ActionListener.runBefore(listener, context::restore));

        // Whether to force deletion of corresponding state
        final boolean clearStatus = Booleans.parseBoolean(request.getParams().get(CLEAR_STATUS), false);
        ActionListener<DeleteResponse> stateListener = ActionListener.wrap(response -> {
            logger.info("Deleted workflow state doc: {}", workflowId);
        }, exception -> { logger.info("Failed to delete workflow state doc: {}", workflowId, exception); });
        flowFrameworkIndicesHandler.canDeleteWorkflowStateDoc(workflowId, clearStatus, canDelete -> {
            if (Boolean.TRUE.equals(canDelete)) {
                flowFrameworkIndicesHandler.deleteFlowFrameworkSystemIndexDoc(workflowId, stateListener);
            }
        }, stateListener);
    }
}
