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
import org.opensearch.flowframework.model.Template;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport Action to provision a workflow from a stored use case template
 */
public class ProvisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {
    private final Logger logger = LogManager.getLogger(ProvisionWorkflowTransportAction.class);
    private final Client client;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param client The node client to retrieve a stored use case template
     */
    @Inject
    public ProvisionWorkflowTransportAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(ProvisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {

        if (request.getWorkflowId() == null) {
            // Workflow provisioning from inline template, first parse and then index the given use case template
            client.execute(CreateWorkflowAction.INSTANCE, request, ActionListener.wrap(workflowResponse -> {
                String workflowId = workflowResponse.getWorkflowId();
                Template template = request.getTemplate();

                // TODO : Use node client to update state index, given workflowId
                // TODO : Pass workflowId and template to execution

                listener.onResponse(new WorkflowResponse(workflowId));

            }, exception -> { listener.onFailure(exception); }));

        } else {

            // Use case template has been previously saved, retrieve entry and execute
            String workflowId = request.getWorkflowId();

            // TODO : use node client to update state index, given workflowId
            // TODO : Retrieve template from global context index using node client execute

            listener.onResponse(new WorkflowResponse(workflowId));
        }
    }

}
