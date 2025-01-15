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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
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

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;

    /**
     * Instantiates a new GetWorkflowStateTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param client The client used to make the request to OS
     * @param xContentRegistry contentRegister to parse get response
     * @param clusterService the cluster service
     * @param settings the plugin settings
     */
    @Inject
    public GetWorkflowStateTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ClusterService clusterService,
        Settings settings
    ) {
        super(GetWorkflowStateAction.NAME, transportService, actionFilters, GetWorkflowStateRequest::new);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, GetWorkflowStateRequest request, ActionListener<GetWorkflowStateResponse> listener) {
        String workflowId = request.getWorkflowId();
        User user = ParseUtils.getUserContext(client);

        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {

            resolveUserAndExecute(
                user,
                workflowId,
                filterByEnabled,
                true,
                listener,
                () -> executeGetWorkflowStateRequest(request, listener, context),
                client,
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
     * @param listener the action listener
     * @param context the thread context
     */
    private void executeGetWorkflowStateRequest(
        GetWorkflowStateRequest request,
        ActionListener<GetWorkflowStateResponse> listener,
        ThreadContext.StoredContext context
    ) {
        String workflowId = request.getWorkflowId();
        GetRequest getRequest = new GetRequest(WORKFLOW_STATE_INDEX).id(workflowId);
        logger.info("Querying state workflow doc: {}", workflowId);
        client.get(getRequest, ActionListener.runBefore(ActionListener.wrap(r -> {
            if (r != null && r.isExists()) {
                try (XContentParser parser = ParseUtils.createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    WorkflowState workflowState = WorkflowState.parse(parser);
                    listener.onResponse(new GetWorkflowStateResponse(workflowState, request.getAll()));
                } catch (Exception e) {
                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to parse workflowState: {}", r.getId())
                        .getFormattedMessage();
                    logger.error(errorMessage, e);
                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                }
            } else {
                listener.onFailure(new FlowFrameworkException("Fail to find workflow status of " + workflowId, RestStatus.NOT_FOUND));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                listener.onFailure(new FlowFrameworkException("Fail to find workflow status of " + workflowId, RestStatus.NOT_FOUND));
            } else {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to get workflow status of: {}", workflowId)
                    .getFormattedMessage();
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
            }
        }), context::restore));
    }
}
