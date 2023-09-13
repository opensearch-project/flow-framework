/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.concurrent.CompletableFuture;

public class DemoWorkflowStep implements WorkflowStep {

    private final long delay;
    private final String name;

    public DemoWorkflowStep(long delay) {
        this.delay = delay;
        this.name = "DEMO_DELAY_" + delay;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(WorkflowData data) {
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(this.delay);
                future.complete(null);
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
