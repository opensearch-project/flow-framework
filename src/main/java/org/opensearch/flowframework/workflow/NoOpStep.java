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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.DELAY_FIELD;

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
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Collections.emptySet();
        Set<String> optionalKeys = Set.of(DELAY_FIELD);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );
            if (inputs.containsKey(DELAY_FIELD)) {
                long delay = TimeValue.parseTimeValue(inputs.get(DELAY_FIELD).toString(), DELAY_FIELD).millis();
                Thread.sleep(delay);
            }
        } catch (IllegalArgumentException iae) {
            throw new WorkflowStepException(iae.getMessage(), RestStatus.BAD_REQUEST);
        } catch (InterruptedException e) {
            FutureUtils.cancel(future);
        }

        future.onResponse(WorkflowData.EMPTY);
        return future;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
