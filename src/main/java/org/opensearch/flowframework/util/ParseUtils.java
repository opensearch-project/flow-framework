/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ml.common.agent.LLMSpec;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.ml.common.utils.StringUtils.getParameterMap;

/**
 * Utility methods for Template parsing
 */
public class ParseUtils {
    private static final Logger logger = LogManager.getLogger(ParseUtils.class);

    public static final Gson gson;

    static {
        gson = new Gson();
    }

    private ParseUtils() {}

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
     * Builds an XContent object representing a LLMSpec.
     *
     * @param xContentBuilder An XContent builder whose position is at the start of the map object to build
     * @param llm LLMSpec object
     * @throws IOException on a build failure
     */
    public static void buildLLMMap(XContentBuilder xContentBuilder, LLMSpec llm) throws IOException {
        String modelId = llm.getModelId();
        Map<String, String> parameters = llm.getParameters();

        xContentBuilder.field(MODEL_ID, modelId);
        xContentBuilder.field(PARAMETERS_FIELD);
        buildStringToStringMap(xContentBuilder, parameters);
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

    public static LLMSpec parseLLM(XContentParser parser) throws IOException {
        String modelId = null;
        Map<String, String> parameters = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case MODEL_ID:
                    modelId = parser.text();
                    break;
                case PARAMETERS_FIELD:
                    parameters = getParameterMap(parser.map());
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return LLMSpec.builder().modelId(modelId).parameters(parameters).build();
    }

    /**
     * Parse content parser to {@link java.time.Instant}.
     *
     * @param parser json based content parser
     * @return instance of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static Instant parseInstant(XContentParser parser) throws IOException {
        if (parser.currentToken() != null && parser.currentToken().isValue() && parser.currentToken() != XContentParser.Token.VALUE_NULL) {
            return Instant.ofEpochMilli(parser.longValue());
        }
        return null;
    }

    /**
     * Generates a user string formed by the username, backend roles, roles and requested tenants separated by '|'
     * (e.g., john||own_index,testrole|__user__, no backend role so you see two verticle line after john.).
     * This is the user string format used internally in the OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT and may be
     * parsed using User.parse(string).
     * @param client Client containing user info. A public API request will fill in the user info in the thread context.
     * @return parsed user object
     */
    public static User getUserContext(Client client) {
        String userStr = client.threadPool().getThreadContext().getTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
        logger.debug("Filtering result by " + userStr);
        return User.parse(userStr);
    }

    public static Map<String, String> getParameterMap(Map<String, ?> parameterObjs) {
        Map<String, String> parameters = new HashMap<>();
        for (String key : parameterObjs.keySet()) {
            Object value = parameterObjs.get(key);
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    if (value instanceof String) {
                        parameters.put(key, (String) value);
                    } else {
                        parameters.put(key, gson.toJson(value));
                    }
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw new RuntimeException(e);
            }
        }
        return parameters;
    }

    /**
     * Creates a XContentParser from a given Registry
     *
     * @param xContentRegistry main registry for serializable content
     * @param bytesReference given bytes to be parsed
     * @return bytesReference of {@link java.time.Instant}
     * @throws IOException IOException if content can't be parsed correctly
     */
    public static XContentParser createXContentParserFromRegistry(NamedXContentRegistry xContentRegistry, BytesReference bytesReference)
        throws IOException {
        return XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, bytesReference, XContentType.JSON);
    }

    /**
     * Generates a string to string Map
     * @param map content map
     * @param fieldName fieldName
     * @return instance of the map
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> getStringToStringMap(Object map, String fieldName) {
        if (map instanceof Map) {
            return (Map<String, String>) map;
        }
        throw new IllegalArgumentException("[" + fieldName + "] must be a key-value map.");
    }

}
