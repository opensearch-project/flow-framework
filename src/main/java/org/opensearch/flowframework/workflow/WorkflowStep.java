/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for the workflow setup of different building blocks.
 */
public interface WorkflowStep {

    /**
     * Triggers the actual processing of the building block.
     * @param data representing input params and content, or output content of previous steps.
     * @return A CompletableFuture of the building block. This block should return immediately, but not be completed until the step executes, containing the step's output data which may be passed to follow-on steps.
     */
    CompletableFuture<WorkflowData> execute(List<WorkflowData> data);

    /**
     *
     * @return the name of this workflow step.
     */
    String getName();

}
