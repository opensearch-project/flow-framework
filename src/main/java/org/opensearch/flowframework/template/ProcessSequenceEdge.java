/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import java.util.Objects;

/**
 * Representation of an edge between process nodes in a workflow graph.
 */
public class ProcessSequenceEdge {
    private final String source;
    private final String destination;

    /**
     * Create this edge with the id's of the source and destination nodes.
     *
     * @param source The source node id.
     * @param destination The destination node id.
     */
    ProcessSequenceEdge(String source, String destination) {
        this.source = source;
        this.destination = destination;
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
        ProcessSequenceEdge other = (ProcessSequenceEdge) obj;
        return Objects.equals(destination, other.destination) && Objects.equals(source, other.source);
    }

    @Override
    public String toString() {
        return this.source + "->" + this.destination;
    }
}
