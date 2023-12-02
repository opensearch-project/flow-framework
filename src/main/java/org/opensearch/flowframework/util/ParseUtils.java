/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.ml.common.agent.LLMSpec;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;

/**
 * Utility methods for Template parsing
 */
public class ParseUtils {
    private static final Logger logger = LogManager.getLogger(ParseUtils.class);

    // Matches ${{ foo.bar }} (whitespace optional) with capturing groups 1=foo, 2=bar
    private static final Pattern SUBSTITUTION_PATTERN = Pattern.compile("\\$\\{\\{\\s*(.+)\\.(.+?)\\s*\\}\\}");

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
     * @param llm LLMSpec
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

    /**
     * Creates a map containing the specified input keys, with values derived from template data or previous node
     * output.
     *
     * @param requiredInputKeys A set of keys that must be present, or will cause an exception to be thrown
     * @param optionalInputKeys A set of keys that may be present, or will be absent in the returned map
     * @param currentNodeInputs Input params and content for this node, from workflow parsing
     * @param outputs WorkflowData content of previous steps
     * @param previousNodeInputs Input params for this node that come from previous steps
     * @return A map containing the requiredInputKeys with their corresponding values,
     *         and optionalInputKeys with their corresponding values if present.
     *         Throws a {@link FlowFrameworkException} if a required key is not present.
     */
    public static Map<String, Object> getInputsFromPreviousSteps(
        Set<String> requiredInputKeys,
        Set<String> optionalInputKeys,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {
        // Mutable set to ensure all required keys are used
        Set<String> requiredKeys = new HashSet<>(requiredInputKeys);
        // Merge input sets to add all requested keys
        Set<String> keys = new HashSet<>(requiredInputKeys);
        keys.addAll(optionalInputKeys);
        // Initialize return map
        Map<String, Object> inputs = new HashMap<>();
        for (String key : keys) {
            Object value = null;
            // Priority 1: specifically named prior step inputs
            // ... parse the previousNodeInputs map and fill in the specified keys
            Optional<String> previousNodeForKey = previousNodeInputs.entrySet()
                .stream()
                .filter(e -> key.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .findAny();
            if (previousNodeForKey.isPresent()) {
                WorkflowData previousNodeOutput = outputs.get(previousNodeForKey.get());
                if (previousNodeOutput != null) {
                    value = previousNodeOutput.getContent().get(key);
                }
            }
            // Priority 2: inputs specified in template
            // ... fetch from currentNodeInputs (params take precedence)
            if (value == null) {
                value = currentNodeInputs.getParams().get(key);
            }
            if (value == null) {
                value = currentNodeInputs.getContent().get(key);
            }
            // Priority 3: other inputs
            if (value == null) {
                Optional<Object> matchedValue = outputs.values()
                    .stream()
                    .map(WorkflowData::getContent)
                    .filter(m -> m.containsKey(key))
                    .map(m -> m.get(key))
                    .findAny();
                if (matchedValue.isPresent()) {
                    value = matchedValue.get();
                }
            }
            // Check for substitution
            if (value != null) {
                Matcher m = SUBSTITUTION_PATTERN.matcher(value.toString());
                if (m.matches()) {
                    WorkflowData data = outputs.get(m.group(1));
                    if (data != null && data.getContent().containsKey(m.group(2))) {
                        value = data.getContent().get(m.group(2));
                    }
                }
                inputs.put(key, value);
                requiredKeys.remove(key);
            }
        }
        // After iterating is complete, throw exception if requiredKeys is not empty
        if (!requiredKeys.isEmpty()) {
            throw new FlowFrameworkException(
                "Missing required inputs "
                    + requiredKeys
                    + " in workflow ["
                    + currentNodeInputs.getWorkflowId()
                    + "] node ["
                    + currentNodeInputs.getNodeId()
                    + "]",
                RestStatus.BAD_REQUEST
            );
        }
        // Finally return the map
        return inputs;
    }
}
