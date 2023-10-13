/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package demo;

import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Demo workflowstep to show sequenced execution
 */
public class DemoWorkflowStep implements WorkflowStep {

    private final long delay;
    private final String name;

    /**
     * Instantiate a step with a delay.
     * @param delay milliseconds to take pretending to do work while really sleeping
     */
    public DemoWorkflowStep(long delay) {
        this.delay = delay;
        this.name = "DEMO_DELAY_" + delay;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(this.delay);
                future.complete(WorkflowData.EMPTY);
            } catch (InterruptedException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public String getName() {
        return name;
    }
}
