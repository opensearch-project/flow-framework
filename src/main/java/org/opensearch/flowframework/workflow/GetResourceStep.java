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
public class GetResourceStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(GetResourceStep.class);
    private final ResourceCreated resourceCreated;

    /** The name of this step */
    public static final String NAME = "get_resource";

    public GetResourceStep(ResourceCreated resourceCreated) {
        this.resourceCreated = resourceCreated;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> getResourceFuture = PlainActionFuture.newFuture();
        getResourceFuture.onResponse(
            new WorkflowData(
                Map.of(resourceCreated.resourceType(), resourceCreated.resourceId()),
                currentNodeInputs.getWorkflowId(),
                currentNodeId
            )
        );
        return getResourceFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
