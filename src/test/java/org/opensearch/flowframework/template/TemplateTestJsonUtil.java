/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Utility methods for tests of template JSON
 */
public class TemplateTestJsonUtil {

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

    public static String parseToJson(ToXContentObject object) throws IOException {
        return object.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS).toString();
    }

    public static XContentParser jsonToParser(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            json
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return parser;
    }
}
