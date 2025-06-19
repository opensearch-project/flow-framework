/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.opensearch.core.common.Strings;
import org.opensearch.flowframework.common.CommonValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

/**
 * Transforms an input JSON into the desired output JSON using
 * a mapping produced by JsonToJsonRecommender.
 *
 * The mapping JSON must have the same structure that the transformer should
 * emit; leaf nodes contain a valid JsonPath expression that references a value
 * inside the input JSON. Both fully-qualified paths (e.g. $.foo.bar)
 * and generalised paths with the [*] wildcard are supported.
 *
 * Example mapping (generalised):
 * <pre>{@code
 * {
 *   "name" : "$.item.name",
 *   "allocDetails[*]" : {
 *     "allocation" : "$.item.allocDetails[0].useful.items[*].allocation",
 *     "team" : {
 *       "id"   : "$.item.allocDetails[0].useful.items[*].team.id",
 *       "name" : "$.item.allocDetails[0].useful.items[*].team.name"
 *     }
 *   }
 * }
 * }</pre>
 * In this example the transformer will copy name directly and
 * create an allocDetails array whose length equals the number of
 * matches produced by the allocation JsonPath. Each array element
 * will contain the corresponding allocation, team.id
 * and team.name values.
 *
 * Implementation notes:
 * - Uses Jackson for JSON tree manipulation and as JsonPath provider.
 * - Uses Jayway JsonPath for value extraction with Jackson as the underlying
 *   JSON provider instead of json-smart to avoid dependency conflicts.
 * - Only [*] wildcards are supported (nested wildcards are
 *   allowed). Explicit numeric indices (e.g. [0]) are honoured
 *   verbatim.
 * - Thread-safe and stateless: all public APIs are static and use local
 *   variables only.
 *
 * Call Flow Architecture:
 * transform()
 *   └── buildOutputFromMapping() [validates object type and processes properties]
 *       ├── processSimpleProperty() [handles JsonPath leaf nodes]
 *       ├── [nested objects] → buildOutputFromMapping() [direct recursion]
 *       └── processArrayProperty() [handles array syntax and builds elements]
 *           └── buildOutputFromMapping() [direct recursion for array elements]
 */
public final class JsonToJsonTransformer {
    private static final ObjectMapper MAPPER;
    private static final Configuration JSON_PATH_CFG;

    // Pre-compiled regex patterns for performance
    private static final Pattern EXPLICIT_INDEX_PATTERN = Pattern.compile("\\[\\d+\\]");
    private static final Pattern WILDCARD_PATTERN = Pattern.compile("\\[\\*\\]");

    static {
        StreamReadConstraints constraints = StreamReadConstraints.builder()
            .maxNestingDepth(CommonValue.MAX_JSON_NESTING_DEPTH)
            .maxStringLength(CommonValue.MAX_JSON_SIZE)
            .maxNameLength(CommonValue.MAX_JSON_NAME_LENGTH)
            .build();

        MAPPER = new ObjectMapper();
        MAPPER.getFactory().setStreamReadConstraints(constraints);

        JSON_PATH_CFG = Configuration.builder()
            .jsonProvider(new JacksonJsonProvider(MAPPER))
            .mappingProvider(new JacksonMappingProvider(MAPPER))
            .options(Option.ALWAYS_RETURN_LIST, Option.SUPPRESS_EXCEPTIONS)
            .build();
    }

    /** Utility class – no instantiation. */
    private JsonToJsonTransformer() {}

    /**
     * Transforms {@code inputJson} into a new JSON document according to the
     * mapping defined in {@code mappingRules}.
     *
     * @param inputJson   source document (string)
     * @param mappingRules mapping produced by JsonToJsonRecommender
     * @return transformed JSON as a string
     * @throws IllegalArgumentException if input parameters are null/empty, input JSON
     *                                 cannot be parsed, or mapping is malformed
     * @throws JsonProcessingException if the output JSON cannot be serialized
     */
    public static String transform(String inputJson, String mappingRules) throws IllegalArgumentException, JsonProcessingException {
        if (Strings.isNullOrEmpty(inputJson)) {
            throw new IllegalArgumentException("inputJson must not be null or empty");
        }
        if (Strings.isNullOrEmpty(mappingRules)) {
            throw new IllegalArgumentException("mappingRules must not be null or empty");
        }

        JsonNode mappingNode = MAPPER.readTree(mappingRules);

        String document;
        try {
            document = JsonPath.using(JSON_PATH_CFG).parse(inputJson).jsonString();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid input JSON for JsonPath parsing", e);
        }

        ObjectNode outputRoot = MAPPER.createObjectNode();
        buildOutputFromMapping(outputRoot, mappingNode, document);

        return MAPPER.writeValueAsString(outputRoot);
    }

    /**
     * Main processing engine that builds output from mapping rules.
     * Processes object-type mapping nodes by delegating to specialized handlers.
     *
     * TERMINATION CONDITIONS:
     * - Only processes JsonNode.isObject() mappings
     * - Throws exception for textual root mappings or unsupported node types
     * - Processes finite set of fields from input mapping
     * - Recursion depth is naturally limited by input structure depth
     *
     * @param outputNode the node to populate
     * @param mappingNode the mapping rules to apply (must be object type)
     * @param inputDocument the source document as JSON string
     */
    private static void buildOutputFromMapping(ObjectNode outputNode, JsonNode mappingNode, String inputDocument) {
        // TERMINATION: Type validation - reject non-object mappings
        if (mappingNode.isTextual()) {
            throw new IllegalArgumentException("Root mapping cannot be a JsonPath string - it must be an object");
        } else if (!mappingNode.isObject()) {
            throw new IllegalArgumentException("Mapping contains unsupported node type: " + mappingNode.getNodeType());
        }

        // Process each property in the object mapping
        Iterator<Map.Entry<String, JsonNode>> fields = mappingNode.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String propertyKey = entry.getKey();
            JsonNode propertyValue = entry.getValue();

            // Determine property type and delegate to appropriate handler
            if (isArrayProperty(propertyKey)) {
                processArrayProperty(outputNode, propertyKey, propertyValue, inputDocument);
            } else if (propertyValue.isTextual()) {
                processSimpleProperty(outputNode, propertyKey, propertyValue, inputDocument);
            } else if (propertyValue.isObject()) {
                // Handle nested object - create nested node and recurse
                ObjectNode nestedOutputNode = outputNode.withObject(propertyKey);
                buildOutputFromMapping(nestedOutputNode, propertyValue, inputDocument);
            } else {
                // TERMINATION: Reject unsupported property value types
                throw new IllegalArgumentException(
                    "Property '" + propertyKey + "' has unsupported value type: " + propertyValue.getNodeType()
                );
            }
        }
    }

    /**
     * Handles simple properties with JsonPath expressions (leaf nodes).
     *
     * TERMINATION: No recursion - pure value extraction and assignment.
     *
     * @param outputNode the target object
     * @param propertyKey the property name
     * @param jsonPathNode the JsonPath expression node
     * @param inputDocument the source document as JSON string
     */
    private static void processSimpleProperty(ObjectNode outputNode, String propertyKey, JsonNode jsonPathNode, String inputDocument) {
        String jsonPath = jsonPathNode.asText();
        List<?> extracted = JsonPath.using(JSON_PATH_CFG).parse(inputDocument).read(jsonPath);

        if (extracted == null || extracted.isEmpty()) {
            return; // No match - skip property
        }

        // If the JsonPath contains [*], always return an array even for single values
        boolean shouldBeArray = jsonPath.endsWith("[*]");

        if (shouldBeArray || extracted.size() > 1) {
            ArrayNode arrayNode = MAPPER.createArrayNode();
            for (Object value : extracted) {
                arrayNode.add(MAPPER.valueToTree(value));
            }
            outputNode.set(propertyKey, arrayNode);
        } else {
            outputNode.set(propertyKey, MAPPER.valueToTree(extracted.get(0)));
        }
    }

    /**
     * Handles array properties (keys containing [*] or [index]).
     *
     * TERMINATION CONDITIONS:
     * - Wildcard arrays: bounded by determineArraySize() result
     * - Explicit index arrays: processes single specified index
     * - Empty size results in no processing
     *
     * @param outputNode the target object
     * @param arrayPropertyKey the array property key (e.g., "items[*]")
     * @param elementMapping the mapping for array elements
     * @param inputDocument the source document as JSON string
     */
    private static void processArrayProperty(
        ObjectNode outputNode,
        String arrayPropertyKey,
        JsonNode elementMapping,
        String inputDocument
    ) {
        ArrayKeyInfo keyInfo = parseArrayKey(arrayPropertyKey);
        ArrayNode arrayNode = outputNode.withArray(keyInfo.propertyName);

        if (keyInfo.isWildcard) {
            // Wildcard array - size determined by actual data
            int arraySize = determineArraySize(elementMapping, inputDocument);

            for (int i = 0; i < arraySize; i++) {
                ObjectNode elementNode = MAPPER.createObjectNode();
                arrayNode.add(elementNode);

                // Build array element with wildcard substitution
                JsonNode processedMapping = substituteWildcardsInMapping(elementMapping, i);
                buildOutputFromMapping(elementNode, processedMapping, inputDocument);
            }
        } else {
            // Explicit index - process single element
            int targetIndex = keyInfo.explicitIndex;

            // Extend array to accommodate index
            while (arrayNode.size() <= targetIndex) {
                arrayNode.add(MAPPER.createObjectNode());
            }

            ObjectNode elementNode = (ObjectNode) arrayNode.get(targetIndex);

            // Build array element without substitution (explicit index paths already have concrete indices)
            buildOutputFromMapping(elementNode, elementMapping, inputDocument);
        }
    }

    /**
     * Determines if a property key represents an array (contains [ and ]).
     *
     * TERMINATION: Simple string analysis, no recursion.
     */
    private static boolean isArrayProperty(String key) {
        return key.contains("[") && key.contains("]");
    }

    /**
     * Parses array key into components.
     *
     * TERMINATION: Simple parsing logic, no recursion.
     */
    private static ArrayKeyInfo parseArrayKey(String arrayKey) {
        int bracketPos = arrayKey.indexOf('[');
        String propertyName = arrayKey.substring(0, bracketPos);
        String indexToken = arrayKey.substring(bracketPos);

        if ("[*]".equals(indexToken)) {
            return new ArrayKeyInfo(propertyName, true, -1);
        } else if (EXPLICIT_INDEX_PATTERN.matcher(indexToken).matches()) {
            int index = Integer.parseInt(indexToken.substring(1, indexToken.length() - 1));
            return new ArrayKeyInfo(propertyName, false, index);
        } else {
            throw new IllegalArgumentException("Unsupported array index format: " + arrayKey);
        }
    }

    /**
     * Recursively substitutes [*] wildcards with specific indices.
     *
     * RECURSION: Only on object node structure, naturally bounded by input depth.
     * TERMINATION: Processes only text and object nodes, finite structure.
     *
     * @param mapping the mapping node to process
     * @param index the index to substitute
     * @return new mapping with substituted indices
     */
    private static JsonNode substituteWildcardsInMapping(JsonNode mapping, int index) {
        if (mapping.isTextual()) {
            String jsonPath = mapping.asText();
            String substituted = WILDCARD_PATTERN.matcher(jsonPath).replaceFirst("[" + index + "]");

            return MAPPER.getNodeFactory().textNode(substituted);
        } else if (mapping.isObject()) {
            ObjectNode result = MAPPER.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = mapping.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                result.set(entry.getKey(), substituteWildcardsInMapping(entry.getValue(), index));
            }
            return result;
        }
        // TERMINATION: Return unchanged for other node types
        return mapping;
    }

    /**
     * Determines array size by finding first wildcard JsonPath and evaluating it.
     *
     * TERMINATION: Searches finite mapping structure, returns definite size.
     *
     * @param elementMapping mapping to analyze
     * @param inputDocument source document as JSON string
     * @return determined array size
     */
    private static int determineArraySize(JsonNode elementMapping, String inputDocument) {
        String samplePath = findFirstWildcardPath(elementMapping);
        if (samplePath != null) {
            List<?> matches = JsonPath.using(JSON_PATH_CFG).parse(inputDocument).read(samplePath);
            return matches != null ? matches.size() : 0;
        }
        return 0;
    }

    /**
     * Finds first JsonPath containing wildcard in mapping structure.
     *
     * RECURSION: Tree traversal, naturally bounded by mapping depth.
     * TERMINATION: Returns on first match or after exhausting structure.
     *
     * @param mapping the mapping node to search
     * @return first wildcard path found, or null
     */
    private static String findFirstWildcardPath(JsonNode mapping) {
        if (mapping.isTextual()) {
            String path = mapping.asText();
            return path.contains("[*]") ? path : null;
        } else if (mapping.isObject()) {
            for (JsonNode element : mapping) {
                String result = findFirstWildcardPath(element);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    /**
     * Encapsulates parsed array key information.
     */
    private static class ArrayKeyInfo {
        final String propertyName;
        final boolean isWildcard;
        final int explicitIndex;

        ArrayKeyInfo(String propertyName, boolean isWildcard, int explicitIndex) {
            this.propertyName = propertyName;
            this.isWildcard = isWildcard;
            this.explicitIndex = explicitIndex;
        }
    }
}
