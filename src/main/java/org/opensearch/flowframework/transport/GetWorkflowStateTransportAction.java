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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.flowframework.util.TenantAwareHelper;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.util.ParseUtils.resolveUserAndExecute;

//TODO: Currently we only get the workflow status but we should change to be able to get the
// full template as well
/**
 * Transport Action to get a specific workflow. Currently, we only support the action with _status
 * in the API path but will add the ability to get the workflow and not just the status in the future
 */
public class GetWorkflowStateTransportAction extends HandledTransportAction<GetWorkflowStateRequest, GetWorkflowStateResponse> {

    private final Logger logger = LogManager.getLogger(GetWorkflowStateTransportAction.class);

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private final Client client;
    private final SdkClient sdkClient;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;

    /**
     * Instantiates a new GetWorkflowStateTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param flowFrameworkIndicesHandler the handler class for index actions
     * @param flowFrameworkSettings the plugin settings
     * @param client The client used to make the request to OS
     * @param sdkClient the Multitenant Client
     * @param xContentRegistry contentRegister to parse get response
     * @param clusterService the cluster service
     * @param settings the plugin settings
     */
    @Inject
    public GetWorkflowStateTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        Client client,
        SdkClient sdkClient,
        NamedXContentRegistry xContentRegistry,
        ClusterService clusterService,
        Settings settings
    ) {
        super(GetWorkflowStateAction.NAME, transportService, actionFilters, GetWorkflowStateRequest::new);
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
        this.client = client;
        this.sdkClient = sdkClient;
        this.xContentRegistry = xContentRegistry;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, GetWorkflowStateRequest request, ActionListener<GetWorkflowStateResponse> listener) {
        String tenantId = request.getTenantId();
        if (!TenantAwareHelper.validateTenantId(flowFrameworkSettings.isMultiTenancyEnabled(), tenantId, listener)) {
            return;
        }
        String workflowId = request.getWorkflowId();
        User user = ParseUtils.getUserContext(client);

        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {

            resolveUserAndExecute(
                user,
                workflowId,
                tenantId,
                filterByEnabled,
                true,
                flowFrameworkSettings.isMultiTenancyEnabled(),
                listener,
                () -> executeGetWorkflowStateRequest(request, tenantId, listener, context),
                client,
                sdkClient,
                clusterService,
                xContentRegistry
            );

        } catch (Exception e) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to get workflow: {}", workflowId)
                .getFormattedMessage();
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

    /**
     * Execute the get workflow state request
     * @param request the get workflow state request
     * @param tenantId the tenant id
     * @param listener the action listener
     * @param context the thread context
     */
    private void executeGetWorkflowStateRequest(
        GetWorkflowStateRequest request,
        String tenantId,
        ActionListener<GetWorkflowStateResponse> listener,
        ThreadContext.StoredContext context
    ) {
        String workflowId = request.getWorkflowId();
        flowFrameworkIndicesHandler.getWorkflowState(workflowId, tenantId, ActionListener.wrap(workflowState -> {
            GetWorkflowStateResponse workflowStateResponse = new GetWorkflowStateResponse(workflowState, request.getAll());
            listener.onResponse(workflowStateResponse);
        }, listener::onFailure), context);
    }
}
