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

import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods to create a JSON string useful for testing nodes and edges
 */
public class GraphJsonUtil {

    public static String node(String id) {
        return "{\"" + WorkflowNode.ID_FIELD + "\": \"" + id + "\", \"" + WorkflowNode.TYPE_FIELD + "\": \"" + "placeholder" + "\"}";
    }

    public static String edge(String sourceId, String destId) {
        return "{\"" + WorkflowEdge.SOURCE_FIELD + "\": \"" + sourceId + "\", \"" + WorkflowEdge.DEST_FIELD + "\": \"" + destId + "\"}";
    }

    public static String workflow(List<String> nodes, List<String> edges) {
        return "{\"workflow\": {" + arrayField(Workflow.NODES_FIELD, nodes) + ", " + arrayField(Workflow.EDGES_FIELD, edges) + "}}";
    }

    private static String arrayField(String fieldName, List<String> objects) {
        return "\"" + fieldName + "\": [" + objects.stream().collect(Collectors.joining(", ")) + "]";
    }

}
