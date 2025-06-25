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

import org.opensearch.flowframework.common.CommonValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for analyzing and mapping relationships between input and
 * output JSON structures.
 *
 * Builds an inverted index from input JSON values to their paths, then uses
 * output JSON values
 * to find matching input paths, producing a mapping from input paths to output
 * fields.
 *
 * Supports nested objects and arrays, and generates both detailed field
 * mappings and generalized
 * JSONPath transformation patterns.
 *
 * All methods are static and stateless. This class is thread-safe.
 */
public class JsonToJsonRecommender {
    /**
     * Private constructor to prevent instantiation of this utility class.
     * All methods are static and should be accessed directly.
     */
    private JsonToJsonRecommender() {
        // Utility class, not meant to be instantiated
    }

    /**
     * ObjectMapper instance used for parsing JSON strings and formatting output.
     */
    private static final ObjectMapper MAPPER;
    static {
        StreamReadConstraints constraints = StreamReadConstraints.builder()
            .maxNestingDepth(CommonValue.MAX_JSON_NESTING_DEPTH)
            .maxStringLength(CommonValue.MAX_JSON_SIZE)
            .maxNameLength(CommonValue.MAX_JSON_NAME_LENGTH)
            .build();

        MAPPER = new ObjectMapper();
        MAPPER.getFactory().setStreamReadConstraints(constraints);
    }

    /**
     * Generates mapping data with default settings.
     * This method generates both detailed field mappings and generalized
     * JSONPath transformation patterns.
     *
     * @param inputJson  Input JSON string to analyze
     * @param outputJson Output JSON string to map against
     * @return StringFormatResult containing transformation recommendations
     * @throws IllegalArgumentException if input validation fails
     * @throws JsonProcessingException  if JSON parsing fails
     */
    public static StringFormatResult getRecommendationInStringFormat(String inputJson, String outputJson) throws IllegalArgumentException,
        JsonProcessingException {
        // Input validation
        validateInputStrings(inputJson, outputJson);

        JsonNode inputNode = MAPPER.readTree(inputJson);
        JsonNode outputNode = MAPPER.readTree(outputJson);

        MapFormatResult result = getRecommendationInMapFormat(inputNode, outputNode, false);

        String detailedJsonPathString = WriteMappedPathAsString(result.detailedJsonPath);
        String generalizedJsonPathString = WriteMappedPathAsString(result.generalizedJsonPath);

        return new StringFormatResult(detailedJsonPathString, generalizedJsonPathString);
    }

    /**
     * Core method that generates mapping recommendations from JsonNode inputs.
     * This is a convenience method that calls
     * {@link #getRecommendationInMapFormat(JsonNode, JsonNode, boolean)}
     * with needValidation set to true.
     *
     * @param inputNode  Input JSON node to analyze
     * @param outputNode Output JSON node to map against
     * @return MapFormatResult containing both detailed and generalized mappings
     * @throws IllegalArgumentException if input validation fails
     */
    public static MapFormatResult getRecommendationInMapFormat(JsonNode inputNode, JsonNode outputNode) throws IllegalArgumentException {
        return getRecommendationInMapFormat(inputNode, outputNode, true);
    }

    /**
     * Core method that generates mapping recommendations from JsonNode inputs.
     * This is the main internal method for getting transformation recommendations
     * from input and output JSON structures.
     *
     * @param inputNode      Input JSON node to analyze
     * @param outputNode     Output JSON node to map against
     * @param needValidation If true, validates the nesting depth and key lengths of JsonNodes;
     *                       otherwise skips validation
     * @return MapFormatResult containing both detailed and generalized mappings as nested objects
     * @throws IllegalArgumentException if input validation fails
     */
    public static MapFormatResult getRecommendationInMapFormat(JsonNode inputNode, JsonNode outputNode, boolean needValidation)
        throws IllegalArgumentException {
        if (needValidation) {
            // Validate nesting depth and key length for both input and output nodes
            validateInputJsonNode(inputNode, "Input JSON Node");
            validateInputJsonNode(outputNode, "Output JSON Node");
        }

        Map<String, String> inputIndex = createInvertedIndex(inputNode);
        return generateMappings(outputNode, inputIndex, "$");
    }

    /**
     * Generates detailed field-to-field mappings for JsonNode inputs
     *
     * @param inputNode  Input JSON node to analyze
     * @param outputNode Output JSON node to map against
     * @return Detailed field-to-field mappings as a nested object structure
     * @throws IllegalArgumentException if input validation fails
     */
    public static Map<String, Object> getRecommendationDetailedInMapFormat(JsonNode inputNode, JsonNode outputNode)
        throws IllegalArgumentException {
        return getRecommendationInMapFormat(inputNode, outputNode).detailedJsonPath;
    }

    /**
     * Generates generalized JSONPath suggestions for JsonNode inputs
     *
     * @param inputNode  Input JSON node to analyze
     * @param outputNode Output JSON node to map against
     * @return Generalized JSONPath suggestions as a nested object structure
     * @throws IllegalArgumentException if input validation fails
     */
    public static Map<String, Object> getRecommendationGeneralizedInMapFormat(JsonNode inputNode, JsonNode outputNode)
        throws IllegalArgumentException {
        return getRecommendationInMapFormat(inputNode, outputNode).generalizedJsonPath;
    }

    /**
     * Writes a nested mapped path structure as a pretty JSON string.
     *
     * @param mappedPath The nested mapped path structure to write
     * @return The mapped path as a pretty JSON string
     * @throws JsonProcessingException if JSON processing fails
     */
    public static String WriteMappedPathAsString(Map<String, Object> mappedPath) throws JsonProcessingException {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(mappedPath);
    }

    /**
     * Generates detailed field-to-field mappings for a given input and output JSON
     *
     * @param inputJson  Input JSON string to analyze
     * @param outputJson Output JSON string to map against
     * @return Detailed field-to-field mappings as a formatted JSON string
     * @throws IllegalArgumentException if input validation fails
     * @throws JsonProcessingException  if JSON parsing fails
     */
    public static String getRecommendationDetailedInStringFormat(String inputJson, String outputJson) throws IllegalArgumentException,
        JsonProcessingException {
        return getRecommendationInStringFormat(inputJson, outputJson).detailedJsonPathString;
    }

    /**
     * Generates generalized JSONPath suggestions for a given input and output JSON
     *
     * @param inputJson  Input JSON string to analyze
     * @param outputJson Output JSON string to map against
     * @return Generalized JSONPath suggestions as a formatted JSON string
     * @throws IllegalArgumentException if input validation fails
     * @throws JsonProcessingException  if JSON parsing fails
     */
    public static String getRecommendationGeneralizedInStringFormat(String inputJson, String outputJson) throws IllegalArgumentException,
        JsonProcessingException {
        return getRecommendationInStringFormat(inputJson, outputJson).generalizedJsonPathString;
    }

    /**
     * Validates input JSON strings for null values, empty strings, and size limits.
     *
     * @param inputJson  Input JSON string to validate
     * @param outputJson Output JSON string to validate
     * @throws IllegalArgumentException if validation fails
     */
    private static void validateInputStrings(String inputJson, String outputJson) throws IllegalArgumentException {
        if (inputJson == null || outputJson == null) {
            throw new IllegalArgumentException("Input and output JSON strings cannot be null");
        }

        if (inputJson.trim().isEmpty() || outputJson.trim().isEmpty()) {
            throw new IllegalArgumentException("Input and output JSON strings cannot be empty");
        }

        // Check if JSON is empty map (ignoring whitespace)
        String trimmedInputJson = inputJson.replaceAll("\\s", "");
        String trimmedOutputJson = outputJson.replaceAll("\\s", "");

        if ("{}".equals(trimmedInputJson) || "{}".equals(trimmedOutputJson)) {
            throw new IllegalArgumentException("JSON cannot be an empty map");
        }

    }

    /**
     * Validates a JsonNode to ensure it doesn't exceed the maximum allowed depth
     * and that all keys don't exceed the maximum allowed length (50000 characters).
     *
     * @param node     The JsonNode to validate
     * @param nodeType A descriptive name for the node type (e.g., "Input JSON Node",
     *                 "Output JSON Node")
     * @throws IllegalArgumentException if the nesting depth or key length exceeds the maximum
     *                                  limit
     */
    private static void validateInputJsonNode(JsonNode node, String nodeType) throws IllegalArgumentException {
        if (node == null) {
            throw new IllegalArgumentException(nodeType + " cannot be null");
        }
        // Validate nesting depth
        int maxDepth = getJsonNodeNestingDepth(node);
        if (maxDepth > CommonValue.MAX_JSON_NESTING_DEPTH) {
            throw new IllegalArgumentException(
                nodeType
                    + " nesting depth ("
                    + maxDepth
                    + ") exceeds the maximum allowed depth ("
                    + CommonValue.MAX_JSON_NESTING_DEPTH
                    + ")"
            );
        }

        // Validate key lengths
        validateJsonNodeKeyLengths(node, nodeType);
    }

    /**
     * Validates that all keys in a JsonNode don't exceed the maximum allowed length (50000 characters) and calculate the maximum nesting depth.
     *
     * @param node     The JsonNode to validate
     * @param nodeType A descriptive name for the node type (e.g., "Input JSON Node",
     *                 "Output JSON Node")
     * @throws IllegalArgumentException if any key length exceeds the maximum limit (50000 characters)
     */
    private static void validateJsonNodeKeyLengths(JsonNode node, String nodeType) throws IllegalArgumentException {
        if (node == null) {
            return;
        }

        if (node.isObject()) {
            var fields = node.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                String key = entry.getKey();
                if (key.length() > 50000) {
                    throw new IllegalArgumentException(
                        nodeType + " contains a key with length (" + key.length() + ") that exceeds the maximum allowed length (50000)"
                    );
                }
                // Recursively validate nested objects
                validateJsonNodeKeyLengths(entry.getValue(), nodeType);
            }
        } else if (node.isArray()) {
            // Validate all array elements
            for (JsonNode arrayElement : node) {
                validateJsonNodeKeyLengths(arrayElement, nodeType);
            }
        }
    }

    /**
     * Calculates the maximum nesting depth of a JsonNode.
     *
     * @param node The JsonNode to analyze
     * @return The maximum nesting depth
     */
    private static int getJsonNodeNestingDepth(JsonNode node) {
        if (node == null || node.isValueNode()) {
            return 0;
        }

        int maxDepth = 0;
        if (node.isObject()) {
            var fields = node.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                int childDepth = getJsonNodeNestingDepth(entry.getValue());
                maxDepth = Math.max(maxDepth, childDepth);
            }
        } else if (node.isArray()) {
            for (JsonNode arrayElement : node) {
                int childDepth = getJsonNodeNestingDepth(arrayElement);
                maxDepth = Math.max(maxDepth, childDepth);
            }
        }

        return maxDepth + 1;
    }

    /**
     * Builds an inverted index for a given JSON node.
     * Each unique value is mapped to a single JSON path where it appears.
     * For duplicate values, only the first occurrence path is stored.
     *
     * @param node The JSON node to process
     * @return A map of values to their JSON paths
     */
    private static Map<String, String> createInvertedIndex(JsonNode node) {
        Map<String, String> invertedIndex = new HashMap<>();
        createIndexRecursive(node, "$", invertedIndex);
        return invertedIndex;
    }

    /**
     * Recursively traverses the JSON node to build the inverted index.
     * This method handles objects, arrays, and primitive values differently to
     * create comprehensive path mappings.
     *
     * @param node          The current JSON node being processed
     * @param path          The JSON path leading to this node
     * @param invertedIndex The inverted index to populate with value-to-path
     *                      mappings
     */
    private static void createIndexRecursive(JsonNode node, String path, Map<String, String> invertedIndex) {
        if (node.isObject()) {
            // Process object properties recursively
            var fields = node.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                String newPath = path.isEmpty() ? entry.getKey() : path + "." + entry.getKey();
                createIndexRecursive(entry.getValue(), newPath, invertedIndex);
            }
        } else if (node.isArray()) {
            // Process each array element with indexed path
            for (int i = 0; i < node.size(); i++) {
                createIndexRecursive(node.get(i), path + "[" + i + "]", invertedIndex);
            }
        } else {
            // Store primitive values with their paths
            String value = node.asText();
            invertedIndex.putIfAbsent(value, path); // Only store the first occurrence of a value
        }
    }

    /**
     * Generates output-to-input mappings by recursively traversing the output JSON
     * node and correlating values with the input index. Creates both detailed and
     * generalized mappings as nested object structures, with special handling for
     * arrays to provide useful transformation patterns.
     *
     * @param outputNode The output JSON node to traverse
     * @param inputIndex The inverted index of input JSON values to paths
     * @param path       The current JSON path being processed (typically starts
     *                   with "$")
     * @return MapFormatResult containing detailed and generalized mappings as nested objects
     */
    private static MapFormatResult generateMappings(JsonNode outputNode, Map<String, String> inputIndex, String path) {
        Map<String, String> detailed = new LinkedHashMap<>();
        Map<String, String> generalized = new LinkedHashMap<>();

        generateMappingsRecursive(outputNode, inputIndex, path, detailed, generalized);

        return new MapFormatResult(toNestedMapping(detailed), toNestedMapping(generalized));
    }

    /**
     * Recursively processes JSON nodes to build output-to-input mappings.
     * Handles special array processing logic where array indices are generalized
     * when elements have similar structures.
     *
     * @param node        The current JSON node being processed
     * @param inputIndex  The inverted index of input JSON values to paths
     * @param path        The current JSON path
     * @param detailed    The detailed mapping accumulator
     * @param generalized The generalized mapping accumulator
     */
    private static void generateMappingsRecursive(
        JsonNode node,
        Map<String, String> inputIndex,
        String path,
        Map<String, String> detailed,
        Map<String, String> generalized
    ) {

        if (node.isObject()) {
            // Process object nodes recursively
            var fields = node.fields();
            while (fields.hasNext()) {
                var entry = fields.next();
                String newPath = path.equals("$") ? "$." + entry.getKey() : path + "." + entry.getKey();
                generateMappingsRecursive(entry.getValue(), inputIndex, newPath, detailed, generalized);
            }

        } else if (node.isArray()) {
            // Process array nodes with special logic for generalization
            List<Map<String, String>> arrayDetailedMappings = new ArrayList<>();
            List<Map<String, String>> arrayGeneralizedMappings = new ArrayList<>();

            // Collect mappings for each array element
            for (int i = 0; i < node.size(); i++) {
                String arrayElementPath = path + "[" + i + "]";
                Map<String, String> elementDetailed = new LinkedHashMap<>();
                Map<String, String> elementGeneralized = new LinkedHashMap<>();

                generateMappingsRecursive(node.get(i), inputIndex, arrayElementPath, elementDetailed, elementGeneralized);

                arrayDetailedMappings.add(elementDetailed);
                arrayGeneralizedMappings.add(elementGeneralized);
            }

            // Check if array elements have similar structure (only array indices differ)
            if (arrayGeneralizedMappings.size() > 1 && areArrayMappingsSimilar(arrayGeneralizedMappings)) {
                // Add all detailed mappings
                for (Map<String, String> elementMapping : arrayDetailedMappings) {
                    detailed.putAll(elementMapping);
                }

                // Add generalized mapping with smart array index replacement
                Map<String, String> firstElementGeneralized = arrayGeneralizedMappings.get(0);
                for (Map.Entry<String, String> entry : firstElementGeneralized.entrySet()) {
                    // Collect all corresponding keys for this entry across array elements
                    Set<String> allKeys = new HashSet<>();
                    Set<String> allValues = new HashSet<>();

                    for (Map<String, String> mapping : arrayGeneralizedMappings) {
                        for (Map.Entry<String, String> e : mapping.entrySet()) {
                            if (e.getKey().replaceAll("\\[\\d+\\]", "[*]").equals(entry.getKey().replaceAll("\\[\\d+\\]", "[*]"))) {
                                allKeys.add(e.getKey());
                                allValues.add(e.getValue());
                            }
                        }
                    }

                    String generalizedKey = generalizeVaryingArrayIndices(allKeys);
                    String generalizedValue = generalizeVaryingArrayIndices(allValues);
                    generalized.put(generalizedKey, generalizedValue);
                }
            } else {
                // Array elements have different structures, add all mappings to both detailed
                // and generalized
                for (int i = 0; i < arrayDetailedMappings.size(); i++) {
                    detailed.putAll(arrayDetailedMappings.get(i));
                    generalized.putAll(arrayGeneralizedMappings.get(i));
                }
            }

        } else {
            // Process leaf values by finding matching input paths
            String value = node.asText();
            String matchingInputPath = inputIndex.get(value);

            if (matchingInputPath != null && !matchingInputPath.isEmpty()) {
                detailed.put(path, matchingInputPath);
                generalized.put(path, matchingInputPath);
            }
        }
    }

    /**
     * Checks if array mappings are similar (only differing in array indices).
     * This determines whether to use generalized array notation or treat each
     * element separately, which is crucial for creating useful transformation
     * patterns.
     *
     * @param arrayMappings List of generalized mappings for array elements
     * @return true if mappings have similar structure with only index differences
     */
    private static boolean areArrayMappingsSimilar(List<Map<String, String>> arrayMappings) {
        if (arrayMappings.size() <= 1) return true;

        Map<String, String> firstMapping = arrayMappings.get(0);
        Set<String> firstKeys = firstMapping.keySet().stream().map(key -> key.replaceAll("\\[\\d+\\]", "[*]")).collect(Collectors.toSet());

        for (int i = 1; i < arrayMappings.size(); i++) {
            Map<String, String> currentMapping = arrayMappings.get(i);
            Set<String> currentKeys = currentMapping.keySet()
                .stream()
                .map(key -> key.replaceAll("\\[\\d+\\]", "[*]"))
                .collect(Collectors.toSet());

            if (!firstKeys.equals(currentKeys)) {
                return false;
            }

            // Check if the values also have similar structure when generalized
            for (String generalizedKey : firstKeys) {
                String firstValue = firstMapping.entrySet()
                    .stream()
                    .filter(e -> e.getKey().replaceAll("\\[\\d+\\]", "[*]").equals(generalizedKey))
                    .map(e -> e.getValue().replaceAll("\\[\\d+\\]", "[*]"))
                    .findFirst()
                    .orElse("");

                String currentValue = currentMapping.entrySet()
                    .stream()
                    .filter(e -> e.getKey().replaceAll("\\[\\d+\\]", "[*]").equals(generalizedKey))
                    .map(e -> e.getValue().replaceAll("\\[\\d+\\]", "[*]"))
                    .findFirst()
                    .orElse("");

                if (!firstValue.equals(currentValue)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Generalizes array indices by replacing only varying indices with [*].
     * This method intelligently identifies which array indices are varying
     * across similar paths and generalizes only those positions.
     *
     * @param paths Set of paths to analyze and generalize
     * @return Generalized path with [*] only for varying array indices
     */
    private static String generalizeVaryingArrayIndices(Set<String> paths) {
        if (paths.isEmpty()) return "";
        if (paths.size() == 1) return paths.iterator().next();

        List<String[]> split = paths.stream().map(p -> p.split("(?<=]\\.)|(?=\\[)|\\.")).collect(Collectors.toList());

        StringBuilder sb = new StringBuilder();
        int max = split.stream().mapToInt(a -> a.length).max().orElse(0);

        for (int i = 0; i < max; i++) {
            int index = i;
            Set<String> parts = split.stream().map(arr -> index < arr.length ? arr[index] : "").collect(Collectors.toSet());

            // Add separator if needed
            if (i > 0 && sb.length() > 0 && !sb.toString().endsWith(".") && !parts.iterator().next().startsWith("[")) {
                sb.append('.');
            }

            if (parts.size() == 1) {
                // All paths have the same part at this position, keep it unchanged
                sb.append(parts.iterator().next());
            } else if (parts.stream().anyMatch(p -> p.matches("\\[\\d+]"))) {
                // Array indices vary at this position, replace with [*]
                sb.append("[*]");
            } else {
                // Other variations, keep the first one
                sb.append(parts.iterator().next());
            }
        }
        return sb.toString();
    }

    /**
     * Safely retrieves or creates a nested Map<String, Object>
     * under a given key.
     * This method ensures type safety when building nested structures and provides
     * clear error messages when there are structural conflicts.
     *
     * @param parent The parent map to get or create the child map in
     * @param key    The key under which to get or create the child map
     * @return The existing or newly created child map
     * @throws IllegalStateException if there's a type conflict at the specified key
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> getOrCreateChildMap(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        if (value == null) {
            Map<String, Object> child = new LinkedHashMap<>();
            parent.put(key, child);
            return child;
        } else if (value instanceof Map) {
            return (Map<String, Object>) value;
        } else {
            throw new IllegalStateException(
                "Conflict at key '" + key + "': expected a nested object but found " + value.getClass().getSimpleName()
            );
        }
    }

    /**
     * Converts a flat Map with dot-separated paths into a nested Map structure.
     * This transformation makes it easier to understand the hierarchical
     * relationship of the mappings and can be used for generating configuration
     * files or templates.
     *
     * Example transformation:
     * Input: "allocDetails[*].team.product.id" =>
     * "$.item.allocDetails.items[*].team.product.id"
     * Output: Nested map structure representing the hierarchy
     *
     * @param flatMap The flat mapping with dot-separated keys and string values
     * @return A nested Map representing the hierarchical structure with object values
     */
    private static Map<String, Object> toNestedMapping(Map<String, String> flatMap) {
        Map<String, Object> result = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : flatMap.entrySet()) {
            String keyPath = entry.getKey();

            // Remove leading "$." if present for cleaner processing
            if (keyPath.startsWith("$.")) {
                keyPath = keyPath.substring(2);
            }

            String[] parts = keyPath.split("\\.");

            // Filter out empty parts to handle edge cases
            List<String> validParts = new ArrayList<>();
            for (String part : parts) {
                if (!part.isEmpty()) {
                    validParts.add(part);
                }
            }

            Map<String, Object> current = result;

            // Build nested structure
            for (int i = 0; i < validParts.size(); i++) {
                String key = validParts.get(i);
                boolean isLast = (i == validParts.size() - 1);

                if (isLast) {
                    current.put(key, entry.getValue());
                } else {
                    current = getOrCreateChildMap(current, key);
                }
            }
        }

        return result;
    }

    /**
     * Class representing the mapping result, containing detailed mappings and
     * JSONPath suggestions. This provides both granular field-to-field mappings
     * and generalized transformation patterns as nested object structures.
     */
    public static class MapFormatResult {
        /** Detailed field-to-field mappings with input paths as nested object structure */
        public final Map<String, Object> detailedJsonPath;

        /** Simplified JSONPath transformation suggestions as nested object structure */
        public final Map<String, Object> generalizedJsonPath;

        /**
         * Constructs a new MapFormatResult with the provided mappings.
         *
         * @param detailedJsonPath    The detailed mapping as nested object structure
         * @param generalizedJsonPath The generalized mapping as nested object structure
         */
        public MapFormatResult(Map<String, Object> detailedJsonPath, Map<String, Object> generalizedJsonPath) {
            this.detailedJsonPath = detailedJsonPath;
            this.generalizedJsonPath = generalizedJsonPath;
        }
    }

    /**
     * Container class for the final mapping output in string format.
     * Provides the transformation recommendations as formatted JSON strings
     * that can be easily consumed by other systems or displayed to users.
     */
    public static class StringFormatResult {
        /** Detailed mapping as a formatted JSON string */
        public final String detailedJsonPathString;

        /** Generalized JSONPath suggestions as a formatted JSON string */
        public final String generalizedJsonPathString;

        /**
         * Constructs a new StringFormatResult with the provided mapping strings.
         *
         * @param detailedJsonPathString    The detailed mapping as JSON string
         * @param generalizedJsonPathString The generalized mapping as JSON string
         */
        public StringFormatResult(String detailedJsonPathString, String generalizedJsonPathString) {
            this.detailedJsonPathString = detailedJsonPathString;
            this.generalizedJsonPathString = generalizedJsonPathString;
        }
    }
}
