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

    @Nullable
    private String workflowId;
    @Nullable
    private String nodeId;

    private WorkflowData() {
        this(Collections.emptyMap(), Collections.emptyMap(), null, null);
    }

    /**
     * Instantiate this object with content and empty params.
     * @param content The content map
     * @param workflowId The workflow ID associated with this step
     * @param nodeId The node ID associated with this step
     */
    public WorkflowData(Map<String, Object> content, @Nullable String workflowId, @Nullable String nodeId) {
        this(content, Collections.emptyMap(), workflowId, nodeId);
    }

    /**
     * Instantiate this object with content and params.
     * @param content The content map
     * @param params The params map
     * @param workflowId The workflow ID associated with this step
     * @param nodeId The node ID associated with this step
     */
    public WorkflowData(Map<String, Object> content, Map<String, String> params, @Nullable String workflowId, @Nullable String nodeId) {
        this.content = Map.copyOf(content);
        this.params = Map.copyOf(params);
        this.workflowId = workflowId;
        this.nodeId = nodeId;
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

    /**
     * Returns the workflowId associated with this data.
     * @return the workflowId of this data.
     */
    @Nullable
    public String getWorkflowId() {
        return this.workflowId;
    };

    /**
     * Returns the nodeId associated with this data.
     * @return the nodeId of this data.
     */
    @Nullable
    public String getNodeId() {
        return this.nodeId;
    };
}
