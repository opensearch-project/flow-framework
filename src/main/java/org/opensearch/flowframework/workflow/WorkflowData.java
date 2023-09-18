/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import java.util.Collections;
import java.util.Map;

/**
 * Interface representing data provided as input to, and produced as output from, {@link WorkflowStep}s.
 */
public interface WorkflowData {

    /**
     * An object representing no data, useful when a workflow step has no required input or output.
     */
    WorkflowData EMPTY = new WorkflowData() {
    };

    /**
     * Accesses a map containing the content of the workflow step. This represents the data associated with a Rest API request.
     * @return the content of this step.
     */
    default Map<String, Object> getContent() {
        return Collections.emptyMap();
    };

    /**
     * Accesses a map containing the params of this workflow step. This represents the params associated with a Rest API request, parsed from the URI.
     * @return the params of this step.
     */
    default Map<String, String> getParams() {
        return Collections.emptyMap();
    };
}
