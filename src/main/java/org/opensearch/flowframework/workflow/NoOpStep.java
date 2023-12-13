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
 * A workflow step that does nothing. May be used for synchronizing other actions.
 */
public class NoOpStep implements WorkflowStep {

    /** Instantiate this class */
    public NoOpStep() {}

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "noop";

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) throws IOException {
        return CompletableFuture.completedFuture(WorkflowData.EMPTY);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
