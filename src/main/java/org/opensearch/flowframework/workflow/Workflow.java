/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

/**
 * Interface for the workflow setup of different building blocks.
 */
public interface Workflow {

    /**
     * Triggers the processing of the building block.
     * @throws Exception if execution fails
     */
    void process() throws Exception;

}
