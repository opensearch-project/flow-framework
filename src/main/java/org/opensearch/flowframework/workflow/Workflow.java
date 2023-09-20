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
import org.opensearch.flowframework.template.WorkflowEdge;
import org.opensearch.flowframework.template.WorkflowNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This represents an object in the workflows section of a {@link Template}.
 */
public class Workflow implements ToXContentFragment {

    private static final String USER_PARAMS_FIELD = "user_params";
    private static final String NODES_FIELD = "nodes";
    private static final String EDGES_FIELD = "edges";

    private Map<String, String> userParams = new HashMap<>();
    private WorkflowNode[] nodes = null;
    private WorkflowEdge[] edges = null;

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

}
