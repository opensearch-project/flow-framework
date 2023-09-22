/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This represents an edge between process nodes (steps) in a workflow graph in the {@link Template}.
 */
public class WorkflowEdge implements ToXContentObject {

    /** The template field name for source node */
    public static final String SOURCE_FIELD = "source";
    /** The template field name for destination node */
    public static final String DEST_FIELD = "dest";

    private final String source;
    private final String destination;

    /**
     * Create this edge with the id's of the source and destination nodes.
     *
     * @param source The source node id.
     * @param destination The destination node id.
     */
    public WorkflowEdge(String source, String destination) {
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
     * Parse raw json content into a workflow edge instance.
     *
     * @param parser json based content parser
     * @return the parsed WorkflowEdge instance
     * @throws IOException if content can't be parsed correctly
     */
    public static WorkflowEdge parse(XContentParser parser) throws IOException {
        String source = null;
        String destination = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case SOURCE_FIELD:
                    source = parser.text();
                    break;
                case DEST_FIELD:
                    destination = parser.text();
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in an edge object.");
            }
        }
        if (source == null || destination == null) {
            throw new IOException("An edge object requires both a source and dest field.");
        }

        return new WorkflowEdge(source, destination);
    }

    /**
     * Gets the source node id.
     *
     * @return the source node id.
     */
    public String source() {
        return source;
    }

    /**
     * Gets the destination node id.
     *
     * @return the destination node id.
     */
    public String destination() {
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
