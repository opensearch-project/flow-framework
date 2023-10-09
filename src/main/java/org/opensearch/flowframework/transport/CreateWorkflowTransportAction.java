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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.indices.GlobalContextHandler;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport Action to index or update a use case template within the Global Context
 */
public class CreateWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(CreateWorkflowTransportAction.class);

    private final GlobalContextHandler globalContextHandler;

    /**
     * Intantiates a new CreateWorkflowTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param globalContextHandler The handler for the global context index
     */
    @Inject
    public CreateWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        GlobalContextHandler globalContextHandler
    ) {
        super(CreateWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.globalContextHandler = globalContextHandler;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {
        if (request.getWorkflowId() == null) {
            // Create new global context and state index entries
            globalContextHandler.putTemplateToGlobalContext(request.getTemplate(), ActionListener.wrap(response -> {
                // TODO : Check if state index exists, create if not
                // TODO : Create StateIndexRequest for workflowId, default to NOT_STARTED
                listener.onResponse(new WorkflowResponse(response.getId()));
            }, exception -> {
                logger.error("Failed to save use case template : {}", exception.getMessage());
                listener.onFailure(exception);
            }));
        } else {
            // Update existing entry, full document replacement
            globalContextHandler.updateTemplate(request.getWorkflowId(), request.getTemplate(), ActionListener.wrap(response -> {
                // TODO : Create StateIndexRequest for workflowId to reset entry to NOT_STARTED
                listener.onResponse(new WorkflowResponse(response.getId()));
            }, exception -> {
                logger.error("Failed to updated use case template {} : {}", request.getWorkflowId(), exception.getMessage());
                listener.onFailure(exception);
            }));
        }
    }

}
