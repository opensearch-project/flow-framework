/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.flowframework.model.ResourceCreated;

import java.util.Map;

/**
 * Internal step to pass created resources to dependent nodes. Only used in reprovisioning
 */
public class WorkflowDataStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(WorkflowDataStep.class);
    private final ResourceCreated resourceCreated;

    /** The name of this step */
    public static final String NAME = "workflow_data_step";

    /**
     * Instantiate this class
     * @param resourceCreated the created resource
     */
    public WorkflowDataStep(ResourceCreated resourceCreated) {
        this.resourceCreated = resourceCreated;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params,
        String tenantId
    ) {
        PlainActionFuture<WorkflowData> workflowDataFuture = PlainActionFuture.newFuture();
        workflowDataFuture.onResponse(
            new WorkflowData(
                Map.of(resourceCreated.resourceType(), resourceCreated.resourceId()),
                currentNodeInputs.getWorkflowId(),
                currentNodeId
            )
        );
        return workflowDataFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
