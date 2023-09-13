/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.common.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for the workflow setup of different building blocks.
 */
public interface WorkflowStep {

    /**
     * Triggers the processing of the building block.
     * @param data for input/output params of the building blocks.
     * @return CompletableFuture of the building block.
     */
    CompletableFuture<WorkflowData> execute(@Nullable WorkflowData data);

    /**
     *
     * @return the name of this workflow step.
     */
    String getName();

}
