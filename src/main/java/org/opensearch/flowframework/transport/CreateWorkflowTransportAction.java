/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;

/**
 * Transport Action to index or update a use case template within the Global Context
 */
public class CreateWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(CreateWorkflowTransportAction.class);

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final Client client;

    /**
     * Intantiates a new CreateWorkflowTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param flowFrameworkIndicesHandler The handler for the global context index
     * @param client The client used to make the request to OS
     */
    @Inject
    public CreateWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        Client client
    ) {
        super(CreateWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {
        User user = getUserContext(client);
        Template templateWithUser = new Template(
            request.getTemplate().name(),
            request.getTemplate().description(),
            request.getTemplate().useCase(),
            request.getTemplate().templateVersion(),
            request.getTemplate().compatibilityVersion(),
            request.getTemplate().workflows(),
            user
        );
        if (request.getWorkflowId() == null) {
            // Create new global context and state index entries
            flowFrameworkIndicesHandler.putTemplateToGlobalContext(templateWithUser, ActionListener.wrap(globalContextResponse -> {
                flowFrameworkIndicesHandler.putInitialStateToWorkflowState(
                    globalContextResponse.getId(),
                    user,
                    ActionListener.wrap(stateResponse -> {
                        logger.info("create state workflow doc");
                        listener.onResponse(new WorkflowResponse(globalContextResponse.getId()));
                    }, exception -> {
                        logger.error("Failed to save workflow state : {}", exception.getMessage());
                        if (exception instanceof FlowFrameworkException) {
                            listener.onFailure(exception);
                        } else {
                            listener.onFailure(new FlowFrameworkException(exception.getMessage(), RestStatus.BAD_REQUEST));
                        }
                    })
                );
            }, exception -> {
                logger.error("Failed to save use case template : {}", exception.getMessage());
                if (exception instanceof FlowFrameworkException) {
                    listener.onFailure(exception);
                } else {
                    listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
                }

            }));
        } else {
            // Update existing entry, full document replacement
            flowFrameworkIndicesHandler.updateTemplateInGlobalContext(
                request.getWorkflowId(),
                request.getTemplate(),
                ActionListener.wrap(response -> {
                    flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                        WORKFLOW_STATE_INDEX,
                        request.getWorkflowId(),
                        ImmutableMap.of(STATE_FIELD, State.NOT_STARTED, PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.NOT_STARTED),
                        ActionListener.wrap(updateResponse -> {
                            logger.info("updated workflow {} state to {}", request.getWorkflowId(), State.NOT_STARTED.name());
                            listener.onResponse(new WorkflowResponse(request.getWorkflowId()));
                        }, exception -> {
                            logger.error("Failed to update workflow state : {}", exception.getMessage());
                            if (exception instanceof FlowFrameworkException) {
                                listener.onFailure(exception);
                            } else {
                                listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
                            }
                        })
                    );
                }, exception -> {
                    logger.error("Failed to updated use case template {} : {}", request.getWorkflowId(), exception.getMessage());
                    if (exception instanceof FlowFrameworkException) {
                        listener.onFailure(exception);
                    } else {
                        listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
                    }

                })
            );
        }
    }

}
