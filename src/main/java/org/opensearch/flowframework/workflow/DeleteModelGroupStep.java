/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Step to delete a model group
 */
public class DeleteModelGroupStep implements WorkflowStep {

    /** Instantiate this class */
    public DeleteModelGroupStep() {}

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "delete_model_group";

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {
        return CompletableFuture.completedFuture(WorkflowData.EMPTY);
    }

    @Override
    public String getName() {
        return NAME;
    }

}
