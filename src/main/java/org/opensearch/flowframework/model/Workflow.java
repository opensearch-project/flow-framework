/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.workflow.WorkflowData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This represents an object in the workflows section of a {@link Template}.
 */
public class Workflow implements ToXContentObject {

    /** The template field name for workflow user params */
    public static final String USER_PARAMS_FIELD = "user_params";
    /** The template field name for workflow nodes */
    public static final String NODES_FIELD = "nodes";
    /** The template field name for workflow edges */
    public static final String EDGES_FIELD = "edges";

    private final Map<String, String> userParams;
    private final List<WorkflowNode> nodes;
    private final List<WorkflowEdge> edges;

    /**
     * Create this workflow with any user params and the graph of nodes and edges.
     *
     * @param userParams A map of user params.
     * @param nodes An array of {@link WorkflowNode} objects
     * @param edges An array of {@link WorkflowEdge} objects.
     */
    public Workflow(Map<String, String> userParams, List<WorkflowNode> nodes, List<WorkflowEdge> edges) {
        this.userParams = Map.copyOf(userParams);
        this.nodes = List.copyOf(nodes);
        this.edges = List.copyOf(edges);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        xContentBuilder.startObject(USER_PARAMS_FIELD);
        for (Entry<String, String> e : userParams.entrySet()) {
            xContentBuilder.field(e.getKey(), e.getValue());
        }
        xContentBuilder.endObject();

        xContentBuilder.startArray(NODES_FIELD);
        for (WorkflowNode n : nodes) {
            xContentBuilder.value(n);
        }
        xContentBuilder.endArray();

        xContentBuilder.startArray(EDGES_FIELD);
        for (WorkflowEdge e : edges) {
            xContentBuilder.value(e);
        }
        xContentBuilder.endArray();

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw JSON content into a workflow instance.
     *
     * @param parser JSON based content parser
     * @return the parsed Workflow instance
     * @throws IOException if content can't be parsed correctly
     */
    public static Workflow parse(XContentParser parser) throws IOException {
        Map<String, String> userParams = new HashMap<>();
        List<WorkflowNode> nodes = new ArrayList<>();
        List<WorkflowEdge> edges = new ArrayList<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case USER_PARAMS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String userParamFieldName = parser.currentName();
                        parser.nextToken();
                        userParams.put(userParamFieldName, parser.text());
                    }
                    break;
                case NODES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        nodes.add(WorkflowNode.parse(parser));
                    }
                    break;
                case EDGES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        edges.add(WorkflowEdge.parse(parser));
                    }
                    break;
            }

        }
        if (nodes.isEmpty()) {
            throw new IOException("A workflow must have at least one node.");
        }
        // Iterate the nodes and infer edges from previous node inputs
        List<WorkflowEdge> inferredEdges = nodes.stream()
            .flatMap(node -> node.previousNodeInputs().keySet().stream().map(previousNode -> new WorkflowEdge(previousNode, node.id())))
            .distinct()
            .collect(Collectors.toList());
        // Remove any that are already in edges list
        inferredEdges.removeAll(edges);
        // Then add them to the edges
        edges.addAll(inferredEdges);
        return new Workflow(userParams, nodes, edges);
    }

    /**
     * Get user parameters. These will be passed to all workflow nodes and available as {@link WorkflowData#getParams()}
     * @return the userParams
     */
    public Map<String, String> userParams() {
        return userParams;
    }

    /**
     * Get the nodes in the workflow. Ordering matches the user template which may or may not match execution order.
     * @return the nodes
     */
    public List<WorkflowNode> nodes() {
        return nodes;
    }

    /**
     * Get the edges in the workflow. These specify connections of nodes which form a graph.
     * @return the edges
     */
    public List<WorkflowEdge> edges() {
        return edges;
    }

    @Override
    public String toString() {
        return "Workflow [userParams=" + userParams + ", nodes=" + nodes + ", edges=" + edges + "]";
    }
}
