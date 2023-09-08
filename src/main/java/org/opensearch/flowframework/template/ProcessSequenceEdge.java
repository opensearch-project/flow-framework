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

public class ProcessSequenceEdge {
    private final String source;
    private final String destination;

    ProcessSequenceEdge(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

    public String getSource() {
        return source;
    }

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
