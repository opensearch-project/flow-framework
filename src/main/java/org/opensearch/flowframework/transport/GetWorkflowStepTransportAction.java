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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STEP;

/**
 * Transport action to retrieve a workflow step json
 */
public class GetWorkflowStepTransportAction extends HandledTransportAction<WorkflowRequest, GetWorkflowStepResponse> {

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
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<GetWorkflowStepResponse> listener) {
        try {
            List<String> steps = request.getParams().size() > 0
                ? Arrays.asList(Strings.splitStringByCommaToArray(request.getParams().get(WORKFLOW_STEP)))
                : Collections.emptyList();
            WorkflowValidator workflowValidator;
            if (steps.isEmpty()) {
                workflowValidator = this.workflowStepFactory.getWorkflowValidator();
            } else {
                workflowValidator = this.workflowStepFactory.getWorkflowValidatorByStep(steps);
            }
            listener.onResponse(new GetWorkflowStepResponse(workflowValidator));
        } catch (Exception e) {
            if (e instanceof FlowFrameworkException) {
                logger.error(e.getMessage());
                listener.onFailure(e);
            }
            logger.error("Failed to retrieve workflow step json.", e);
            listener.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
        }
    }
}
