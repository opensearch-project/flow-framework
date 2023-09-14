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

/**
 * Interface for handling the input/output of the building blocks.
 */
public interface WorkflowData {

    /**
     * Accesses a map containing the content of the workflow step. This represents the data associated with a Rest API request.
     * @return the content of this step.
     */
    Map<String, Object> getContent();
}
