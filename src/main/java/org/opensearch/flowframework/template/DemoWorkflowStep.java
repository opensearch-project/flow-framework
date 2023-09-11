/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.flowframework.workflow.Workflow;

import java.util.concurrent.CompletableFuture;

public class DemoWorkflowStep implements Workflow {

    private final long delay;

    public DemoWorkflowStep(long delay) {
        this.delay = delay;
    }

    @Override
    public CompletableFuture<Workflow> execute() throws Exception {
        Thread.sleep(this.delay);
        return null;
    }

}
