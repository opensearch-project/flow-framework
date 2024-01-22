/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.support.PlainActionFuture;

import java.util.Map;

/**
 * A workflow step that does nothing. May be used for synchronizing other actions.
 */
public class NoOpStep implements WorkflowStep {

    /** Instantiate this class */
    public NoOpStep() {}

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "noop";

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {
        PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();
        future.onResponse(WorkflowData.EMPTY);
        return future;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
