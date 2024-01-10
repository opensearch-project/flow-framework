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
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to retrieve a workflow step json
 */
public class GetWorkflowStepTransportAction extends HandledTransportAction<ActionRequest, GetWorkflowStepResponse> {

    private final Logger logger = LogManager.getLogger(GetWorkflowStepTransportAction.class);

    /**
     * Instantiates a new GetWorkflowStepTransportAction instance
     * @param transportService the transport service
     * @param actionFilters action filters
     */
    @Inject
    public GetWorkflowStepTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(GetWorkflowStepAction.NAME, transportService, actionFilters, WorkflowRequest::new);
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<GetWorkflowStepResponse> listener) {
        try {
            listener.onResponse(new GetWorkflowStepResponse(WorkflowValidator.parse("mappings/workflow-steps.json")));
        } catch (Exception e) {
            logger.error("Failed to retrieve workflow step json.", e);
            listener.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
        }
    }
}
