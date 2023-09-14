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
 * Interface for handling the input of the building blocks.
 */
public interface WorkflowInputData extends WorkflowData {

    /**
     * Accesses a map containing the params of this workflow step. This represents the params associated with a Rest API request, parsed from the URI.
     * @return the params of this step.
     */
    Map<String, String> getParams();

}
