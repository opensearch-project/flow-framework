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
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to retrieve a workflow step json
 */
public class GetWorkflowStepTransportAction extends HandledTransportAction<ActionRequest, GetWorkflowStepResponse> {

    private final Logger logger = LogManager.getLogger(GetWorkflowStepTransportAction.class);
    private final WorkflowStepFactory workflowStepFactory;

    /**
     * Instantiates a new GetWorkflowStepTransportAction instance
     * @param transportService the transport service
     * @param actionFilters action filters
     * @param workflowStepFactory The factory instantiating workflow steps
     */
    @Inject
    public GetWorkflowStepTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        WorkflowStepFactory workflowStepFactory
    ) {
        super(GetWorkflowStepAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.workflowStepFactory = workflowStepFactory;
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<GetWorkflowStepResponse> listener) {
        try {
            WorkflowValidator workflowValidator = this.workflowStepFactory.getWorkflowValidator();
            listener.onResponse(new GetWorkflowStepResponse(workflowValidator));
        } catch (Exception e) {
            logger.error("Failed to retrieve workflow step json.", e);
            listener.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
        }
    }
}
