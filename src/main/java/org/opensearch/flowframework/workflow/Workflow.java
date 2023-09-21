/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.template.WorkflowEdge;
import org.opensearch.flowframework.template.WorkflowNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This represents an object in the workflows section of a {@link Template}.
 */
public class Workflow implements ToXContentFragment {

    private static final String USER_PARAMS_FIELD = "user_params";
    private static final String NODES_FIELD = "nodes";
    // TODO: private static final String STEPS_FIELD = "steps";
    private static final String EDGES_FIELD = "edges";

    private final Map<String, String> userParams;
    private final WorkflowNode[] nodes;
    private final WorkflowEdge[] edges;

    public Workflow(Map<String, String> userParams, WorkflowNode[] nodes, WorkflowEdge[] edges) {
        this.userParams = userParams;
        this.nodes = nodes;
        this.edges = edges;
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
     * Parse raw json content into a workflow instance.
     *
     * @param parser json based content parser
     * @throws IOException if content can't be parsed correctly
     */
    public static Workflow parse(XContentParser parser) throws IOException {
        Map<String, String> userParams = new HashMap<>();
        WorkflowNode[] nodes = null;
        WorkflowEdge[] edges = null;

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
                    List<WorkflowNode> nodesList = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        nodesList.add(WorkflowNode.parse(parser));
                    }
                    nodes = nodesList.toArray(new WorkflowNode[0]);
                    break;
                case EDGES_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    List<WorkflowEdge> edgesList = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        edgesList.add(WorkflowEdge.parse(parser));
                    }
                    edges = edgesList.toArray(new WorkflowEdge[0]);
                    break;
            }

        }
        if (nodes == null || nodes.length == 0) {
            throw new IOException("A workflow must have at least one node.");
        }
        // TODO: if edges are empty, create edges by iterating over nodes and adding one between each pair
        return new Workflow(userParams, nodes, edges);
    }
}
