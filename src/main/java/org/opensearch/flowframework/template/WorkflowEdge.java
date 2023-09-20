/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * This represents an edge between process nodes (steps) in a workflow graph in the {@link Template}.
 */
public class WorkflowEdge implements ToXContentFragment {
    public static final String DEST_FIELD = "dest";
    public static final String SOURCE_FIELD = "source";

    private final String source;
    private final String destination;

    /**
     * Create this edge with the id's of the source and destination nodes.
     *
     * @param source The source node id.
     * @param destination The destination node id.
     */
    WorkflowEdge(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(SOURCE_FIELD, this.source);
        xContentBuilder.field(DEST_FIELD, this.destination);
        return xContentBuilder.endObject();
    }

    /**
     * Gets the source node id.
     *
     * @return the source node id.
     */
    public String getSource() {
        return source;
    }

    /**
     * Gets the destination node id.
     *
     * @return the destination node id.
     */
    public String getDestination() {
        return destination;
    }

    @Override
    public int hashCode() {
        return Objects.hash(destination, source);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        WorkflowEdge other = (WorkflowEdge) obj;
        return Objects.equals(destination, other.destination) && Objects.equals(source, other.source);
    }

    @Override
    public String toString() {
        return this.source + "->" + this.destination;
    }
}
