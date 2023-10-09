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
 * Class encapsulating data provided as input to, and produced as output from, {@link WorkflowStep}s.
 */
public class WorkflowData {

    /**
     * An object representing no data, useful when a workflow step has no required input or output.
     */
    public static WorkflowData EMPTY = new WorkflowData();

    private final Map<String, Object> content;
    private final Map<String, String> params;

    private WorkflowData() {
        this(Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Instantiate this object with content and empty params.
     * @param content The content map
     */
    public WorkflowData(Map<String, Object> content) {
        this(content, Collections.emptyMap());
    }

    /**
     * Instantiate this object with content and params.
     * @param content The content map
     * @param params The params map
     */
    public WorkflowData(Map<String, Object> content, Map<String, String> params) {
        this.content = Map.copyOf(content);
        this.params = Map.copyOf(params);
    }

    /**
     * Returns a map which represents the content associated with a Rest API request or response.
     *
     * @return the content of this data.
     */
    public Map<String, Object> getContent() {
        return this.content;
    };

    /**
     * Returns a map represents the params associated with a Rest API request, parsed from the URI.
     * @return the params of this data.
     */
    public Map<String, String> getParams() {
        return this.params;
    };
}
