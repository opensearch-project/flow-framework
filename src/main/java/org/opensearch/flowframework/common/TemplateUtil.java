/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Utility methods for Template parsing
 */
public class TemplateUtil {

    private TemplateUtil() {}

    /**
     * Converts a JSON string into an XContentParser
     *
     * @param json the json string
     * @return The XContent parser for the json string
     * @throws IOException on failure to create the parser
    */
    public static XContentParser jsonToParser(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            json
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return parser;
    }

    /**
     * Builds an XContent object representing a map of String keys to String values.
     *
     * @param xContentBuilder An XContent builder whose position is at the start of the map object to build
     * @param map A map as key-value String pairs.
     * @throws IOException on a build failure
     */
    public static void buildStringToStringMap(XContentBuilder xContentBuilder, Map<?, ?> map) throws IOException {
        xContentBuilder.startObject();
        for (Entry<?, ?> e : map.entrySet()) {
            xContentBuilder.field((String) e.getKey(), (String) e.getValue());
        }
        xContentBuilder.endObject();
    }

    /**
     * Parses an XContent object representing a map of String keys to String values.
     *
     * @param parser An XContent parser whose position is at the start of the map object to parse
     * @return A map as identified by the key-value pairs in the XContent
     * @throws IOException on a parse failure
     */
    public static Map<String, String> parseStringToStringMap(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        Map<String, String> map = new HashMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            map.put(fieldName, parser.text());
        }
        return map;
    }

}
