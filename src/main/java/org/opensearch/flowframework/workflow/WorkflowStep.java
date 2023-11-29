/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for the workflow setup of different building blocks.
 */
public interface WorkflowStep {

    /**
     * Triggers the actual processing of the building block.
     * @param currentNodeId The id of the node executing this step
     * @param currentNodeInputs Input params and content for this node, from workflow parsing
     * @param previousNodeInputs Input params for this node that come from previous steps
     * @param outputs WorkflowData content of previous steps.
     * @return A CompletableFuture of the building block. This block should return immediately, but not be completed until the step executes, containing either the step's output data or {@link WorkflowData#EMPTY} which may be passed to follow-on steps.
     * @throws IOException on a failure.
     */
    CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) throws IOException;

    /**
     * Gets the name of the workflow step.
     * @return the name of this workflow step.
     */
    String getName();
}
