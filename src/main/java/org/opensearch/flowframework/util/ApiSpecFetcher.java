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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flowframework.exception.ApiSpecParseException;
import org.opensearch.rest.RestRequest;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;

/**
 * Utility class for fetching and parsing OpenAPI specifications.
 */
public class ApiSpecFetcher {
    private static final Logger logger = LogManager.getLogger(ApiSpecFetcher.class);
    private static final ParseOptions OPTIMIZED_PARSE_OPTIONS = new ParseOptions();
    private static final OpenAPIV3Parser OPENAPI_PARSER = new OpenAPIV3Parser();

    // Cache for parsed OpenAPI specs to avoid repeated parsing
    private static final ConcurrentMap<String, OpenAPI> SPEC_CACHE = new ConcurrentHashMap<>();

    // Cache for required fields to avoid repeated extraction
    private static final ConcurrentMap<String, List<String>> REQUIRED_FIELDS_CACHE = new ConcurrentHashMap<>();

    static {
        // Ultra-minimal parsing options to prevent memory issues
        OPTIMIZED_PARSE_OPTIONS.setResolve(false); // Disable all resolution
        OPTIMIZED_PARSE_OPTIONS.setResolveFully(false); // Disable full resolution
        OPTIMIZED_PARSE_OPTIONS.setResolveCombinators(false); // Disable combinators resolution
        OPTIMIZED_PARSE_OPTIONS.setFlatten(false); // Don't flatten the spec
        OPTIMIZED_PARSE_OPTIONS.setValidateExternalRefs(false); // Don't validate external refs
    }

    private ApiSpecFetcher() {}

    /**
     * Parses the OpenAPI specification directly from the URI.
     * Uses caching and optimized parsing to avoid memory issues.
     *
     * @param apiSpecUri URI to the API specification (can be file path or web URI).
     * @return Parsed OpenAPI object.
     * @throws ApiSpecParseException If parsing fails.
     */
    public static OpenAPI fetchApiSpec(String apiSpecUri) {
        return fetchApiSpec(apiSpecUri, true);
    }

    /**
     * Parses the OpenAPI specification directly from the URI with configurable resolution.
     *
     * @param apiSpecUri URI to the API specification (can be file path or web URI).
     * @param useOptimizedParsing If true, uses optimized parsing to reduce memory usage.
     * @return Parsed OpenAPI object.
     * @throws ApiSpecParseException If parsing fails.
     */
    public static OpenAPI fetchApiSpec(String apiSpecUri, boolean useOptimizedParsing) {
        logger.info("Parsing API spec from URI: {}", apiSpecUri);

        // Check cache first
        OpenAPI cachedSpec = SPEC_CACHE.get(apiSpecUri);
        if (cachedSpec != null) {
            logger.debug("Using cached API spec for URI: {}", apiSpecUri);
            return cachedSpec;
        }

        SwaggerParseResult result = OPENAPI_PARSER.readLocation(apiSpecUri, null, OPTIMIZED_PARSE_OPTIONS);
        OpenAPI openApi = result.getOpenAPI();

        if (openApi == null) {
            throw new ApiSpecParseException("Unable to parse spec from URI: " + apiSpecUri, result.getMessages());
        }

        // Cache the parsed spec for future use
        SPEC_CACHE.put(apiSpecUri, openApi);

        return openApi;
    }

    /**
     * Compares the required fields in the API spec with the required enum parameters.
     * Uses optimized Swagger parsing with caching to avoid memory issues.
     *
     * @param requiredEnumParams List of required parameters from the enum.
     * @param apiSpecUri URI of the API spec to fetch and compare.
     * @param path The API path to check.
     * @param method The HTTP method (POST, GET, etc.).
     * @return boolean indicating if the required fields match.
     */
    public static boolean compareRequiredFields(List<String> requiredEnumParams, String apiSpecUri, String path, RestRequest.Method method)
        throws IllegalArgumentException, ApiSpecParseException {

        // Create cache key for this specific request
        String cacheKey = apiSpecUri + ":" + path + ":" + method.name();

        // Check cache first
        List<String> cachedRequiredFields = REQUIRED_FIELDS_CACHE.get(cacheKey);
        if (cachedRequiredFields != null) {
            logger.debug("Using cached required fields for: {}", cacheKey);
            return cachedRequiredFields.stream().allMatch(requiredEnumParams::contains);
        }

        // Parse the OpenAPI spec using optimized settings
        OpenAPI openAPI = fetchApiSpec(apiSpecUri, true);

        PathItem pathItem = openAPI.getPaths().get(path);
        if (pathItem == null) {
            throw new IllegalArgumentException("Path not found in API spec: " + path);
        }

        List<String> requiredApiParams = null;

        try {
            Content content = getContent(method, pathItem);
            if (content == null) {
                // Handle case where requestBody uses $ref that wasn't resolved
                return handleUnresolvedRequestBody(openAPI, pathItem, method, requiredEnumParams, cacheKey);
            }

            MediaType mediaType = content.get(XContentType.JSON.mediaTypeWithoutParameters());

            if (mediaType != null) {
                Schema<?> schema = mediaType.getSchema();
                if (schema != null) {
                    requiredApiParams = schema.getRequired();
                }
            }
        } catch (ApiSpecParseException e) {
            // Re-throw the exception if it's about missing requestBody for operations that should have one
            if (e.getMessage().contains("No requestBody defined for this operation")) {
                throw e;
            }
            // Handle case where requestBody uses $ref that wasn't resolved
            return handleUnresolvedRequestBody(openAPI, pathItem, method, requiredEnumParams, cacheKey);
        }

        // Cache the result for future use
        if (requiredApiParams != null) {
            REQUIRED_FIELDS_CACHE.put(cacheKey, requiredApiParams);
            logger.debug("Required enum params: {}", requiredEnumParams);
            logger.debug("Required API params: {}", requiredApiParams);
            return requiredApiParams.stream().allMatch(requiredEnumParams::contains);
        }

        return false;
    }

    /**
     * Handles cases where requestBody uses $ref that wasn't resolved due to minimal parsing.
     * This method manually extracts required fields from the components section.
     */
    private static boolean handleUnresolvedRequestBody(
        OpenAPI openAPI,
        PathItem pathItem,
        RestRequest.Method method,
        List<String> requiredEnumParams,
        String cacheKey
    ) {
        try {
            Operation operation = getOperation(method, pathItem);
            if (operation == null) {
                return false;
            }

            RequestBody requestBody = operation.getRequestBody();
            if (requestBody == null) {
                return false;
            }

            // Check if requestBody has a $ref
            String ref = requestBody.get$ref();
            if (ref != null && ref.startsWith("#/components/requestBodies/")) {
                String componentName = ref.substring("#/components/requestBodies/".length());
                List<String> requiredFields = extractRequiredFieldsFromComponent(openAPI, componentName);

                if (requiredFields != null && !requiredFields.isEmpty()) {
                    // Cache the result
                    REQUIRED_FIELDS_CACHE.put(cacheKey, requiredFields);
                    logger.debug("Required enum params: {}", requiredEnumParams);
                    logger.debug("Required API params from component: {}", requiredFields);
                    return requiredFields.stream().allMatch(requiredEnumParams::contains);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to handle unresolved requestBody: {}", e.getMessage());
        }

        return false;
    }

    /**
     * Gets the operation for the specified method from the path item.
     */
    private static Operation getOperation(RestRequest.Method method, PathItem pathItem) {
        switch (method) {
            case POST:
                return pathItem.getPost();
            case GET:
                return pathItem.getGet();
            case PUT:
                return pathItem.getPut();
            case DELETE:
                return pathItem.getDelete();
            default:
                return null;
        }
    }

    /**
     * Extracts required fields from a component definition in the OpenAPI spec.
     */
    private static List<String> extractRequiredFieldsFromComponent(OpenAPI openAPI, String componentName) {
        if (openAPI.getComponents() == null || openAPI.getComponents().getRequestBodies() == null) {
            return null;
        }

        RequestBody requestBodyComponent = openAPI.getComponents().getRequestBodies().get(componentName);
        if (requestBodyComponent == null) {
            return null;
        }

        Content content = requestBodyComponent.getContent();
        if (content == null) {
            return null;
        }

        MediaType mediaType = content.get(XContentType.JSON.mediaTypeWithoutParameters());
        if (mediaType == null) {
            return null;
        }

        Schema<?> schema = mediaType.getSchema();
        if (schema == null) {
            return null;
        }

        return schema.getRequired();
    }

    private static Content getContent(RestRequest.Method method, PathItem pathItem) throws IllegalArgumentException, ApiSpecParseException {
        Operation operation;
        switch (method) {
            case POST:
                operation = pathItem.getPost();
                break;
            case GET:
                operation = pathItem.getGet();
                break;
            case PUT:
                operation = pathItem.getPut();
                break;
            case DELETE:
                operation = pathItem.getDelete();
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        }

        if (operation == null) {
            throw new IllegalArgumentException("No operation found for the specified method: " + method);
        }

        RequestBody requestBody = operation.getRequestBody();
        if (requestBody == null) {
            throw new ApiSpecParseException("No requestBody defined for this operation.");
        }

        return requestBody.getContent();
    }
}
