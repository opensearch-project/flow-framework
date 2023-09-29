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
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport Action to index or update a use case template within the Global Context
 */
public class CreateWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(CreateWorkflowTransportAction.class);

    private final Client client;

    /**
     * Intantiates a new CreateWorkflowTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param client the node client to interact with an index
     */
    @Inject
    public CreateWorkflowTransportAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(CreateWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {

        String workflowId;
        // TODO : Check if global context index exists, and if it does not then create

        if (request.getWorkflowId() == null) {
            // TODO : Create new entry
            // TODO : Insert doc

            // TODO : get document ID
            workflowId = "";
            // TODO : check if state index exists, and if it does not, then create
            // TODO : insert state index doc, mapped with documentId, defaulted to NOT_STARTED
        } else {
            // TODO : Update existing entry
            workflowId = request.getWorkflowId();
            // TODO : Update state index entry, default back to NOT_STARTED
        }

        listener.onResponse(new WorkflowResponse(workflowId));
    }

}
