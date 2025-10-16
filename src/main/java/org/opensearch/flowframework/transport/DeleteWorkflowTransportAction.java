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
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.TenantAwareHelper;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import static org.opensearch.flowframework.common.CommonValue.CLEAR_STATUS;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;
import static org.opensearch.flowframework.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.flowframework.util.ParseUtils.verifyResourceAccessAndProcessRequest;

/**
 * Transport action to retrieve a use case template within the Global Context
 */
public class DeleteWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, DeleteResponse> {

    private final Logger logger = LogManager.getLogger(DeleteWorkflowTransportAction.class);

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private final Client client;
    private final SdkClient sdkClient;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new DeleteWorkflowTransportAction instance
     * @param transportService the transport service
     * @param actionFilters action filters
     * @param flowFrameworkIndicesHandler The Flow Framework indices handler
     * @param flowFrameworkSettings The Flow Framework settings
     * @param client the OpenSearch Client
     * @param sdkClient the Multitenant Client
     * @param clusterService the cluster service
     * @param xContentRegistry contentRegister to parse get response
     * @param settings the plugin settings
     */
    @Inject
    public DeleteWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        Client client,
        SdkClient sdkClient,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings
    ) {
        super(DeleteWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
        this.client = client;
        this.sdkClient = sdkClient;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<DeleteResponse> listener) {
        if (flowFrameworkIndicesHandler.doesIndexExist(GLOBAL_CONTEXT_INDEX)) {
            String tenantId = request.getTemplate() == null ? null : request.getTemplate().getTenantId();
            if (!TenantAwareHelper.validateTenantId(flowFrameworkSettings.isMultiTenancyEnabled(), tenantId, listener)) {
                return;
            }
            String workflowId = request.getWorkflowId();
            User user = getUserContext(client);

            final boolean clearStatus = Booleans.parseBoolean(request.getParams().get(CLEAR_STATUS), false);

            ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext();

            verifyResourceAccessAndProcessRequest(
                CommonValue.WORKFLOW_RESOURCE_TYPE,
                () -> executeDeleteRequest(request, tenantId, listener, context),
                () -> resolveUserAndExecute(
                    user,
                    workflowId,
                    tenantId,
                    filterByEnabled,
                    clearStatus,
                    flowFrameworkSettings.isMultiTenancyEnabled(),
                    listener,
                    () -> executeDeleteRequest(request, tenantId, listener, context),
                    client,
                    sdkClient,
                    clusterService,
                    xContentRegistry
                )
            );
        } else {
            String errorMessage = "There are no templates in the global context";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        }
    }

    /**
     * Executes the delete request
     * @param request the workflow request
     * @param tenantId
     * @param listener the action listener
     * @param context the thread context
     */
    private void executeDeleteRequest(
        WorkflowRequest request,
        String tenantId,
        ActionListener<DeleteResponse> listener,
        ThreadContext.StoredContext context
    ) {
        String workflowId = request.getWorkflowId();
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
            .index(GLOBAL_CONTEXT_INDEX)
            .id(workflowId)
            .tenantId(tenantId)
            .build();
        sdkClient.deleteDataObjectAsync(deleteRequest).whenComplete((r, throwable) -> {
            context.restore();
            if (throwable == null) {
                try {
                    DeleteResponse response = DeleteResponse.fromXContent(r.parser());
                    listener.onResponse(response);
                } catch (Exception e) {
                    logger.error("Failed to parse delete response", e);
                    listener.onFailure(new FlowFrameworkException("Failed to parse delete response", RestStatus.INTERNAL_SERVER_ERROR));
                }
            } else {
                Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to delete template {}", workflowId)
                    .getFormattedMessage();
                logger.error(errorMessage, exception);
                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR));
            }
        });

        // Whether to force deletion of corresponding state
        final boolean clearStatus = Booleans.parseBoolean(request.getParams().get(CLEAR_STATUS), false);
        ActionListener<DeleteResponse> stateListener = ActionListener.wrap(response -> {
            logger.info("Deleted workflow state doc: {}", workflowId);
        }, exception -> { logger.info("Failed to delete workflow state doc: {}", workflowId, exception); });
        flowFrameworkIndicesHandler.canDeleteWorkflowStateDoc(workflowId, tenantId, clearStatus, canDelete -> {
            if (Boolean.TRUE.equals(canDelete)) {
                flowFrameworkIndicesHandler.deleteFlowFrameworkSystemIndexDoc(workflowId, tenantId, stateListener);
            }
        }, stateListener);
    }
}
