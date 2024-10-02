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
import org.opensearch.flowframework.exception.ApiSpecParseException;
import org.opensearch.rest.RestRequest;

import java.net.URI;
import java.util.HashSet;
import java.util.List;

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
    private static final Logger LOGGER = LogManager.getLogger(ApiSpecFetcher.class);
    private static final ParseOptions PARSE_OPTIONS = new ParseOptions();
    private static final OpenAPIV3Parser PARSER = new OpenAPIV3Parser();

    /**
     * Default constructor for ApiSpecFetcher.
     * It sets up default parse options for resolving references.
     */
    public ApiSpecFetcher() {
        PARSE_OPTIONS.setResolve(true);
        PARSE_OPTIONS.setResolveFully(true);
    }

    /**
     * Parses the OpenAPI specification directly from the URI.
     *
     * @param apiSpecUri URI to the API specification (can be file path or web URI).
     * @return Parsed OpenAPI object.
     * @throws ApiSpecParseException If parsing fails.
     */
    public OpenAPI fetchApiSpec(URI apiSpecUri) {
        LOGGER.info("Parsing API spec from URI: {}", apiSpecUri);
        SwaggerParseResult result = PARSER.readLocation(apiSpecUri.toString(), null, PARSE_OPTIONS);
        OpenAPI openApi = result.getOpenAPI();

        if (openApi == null) {
            throw new ApiSpecParseException("Unable to parse spec from URI: " + apiSpecUri, String.join(", ", result.getMessages()));
        }

        return openApi;
    }

    /**
     * Compares the required fields in the API spec with the required enum parameters.
     *
     * @param requiredEnumParams List of required parameters from the enum.
     * @param apiSpecUri URI of the API spec to fetch and compare.
     * @param path The API path to check.
     * @param method The HTTP method (POST, GET, etc.).
     * @return boolean indicating if the required fields match.
     * @throws Exception If fetching or parsing fails.
     */
    public boolean compareRequiredFields(List<String> requiredEnumParams, URI apiSpecUri, String path, RestRequest.Method method)
        throws Exception {
        OpenAPI openAPI = fetchApiSpec(apiSpecUri);

        PathItem pathItem = openAPI.getPaths().get(path);
        Content content = getContent(method, pathItem);
        MediaType mediaType = content.get("application/json");
        if (mediaType != null) {
            Schema<?> schema = mediaType.getSchema();

            List<String> requiredApiParams = schema.getRequired();
            if (requiredApiParams != null && !requiredApiParams.isEmpty()) {
                return new HashSet<>(requiredEnumParams).equals(new HashSet<>(requiredApiParams));
            }
        }
        return false;
    }

    private static Content getContent(RestRequest.Method method, PathItem pathItem) throws Exception {
        Operation operation = switch (method) {
            case RestRequest.Method.POST -> pathItem.getPost();
            case RestRequest.Method.GET -> pathItem.getGet();
            case RestRequest.Method.PUT -> pathItem.getPut();
            case RestRequest.Method.DELETE -> pathItem.getDelete();
            default -> throw new Exception("Unsupported HTTP method: " + method);
        };

        if (operation == null) {
            throw new Exception("No operation found for the specified method: " + method);
        }

        RequestBody requestBody = operation.getRequestBody();
        if (requestBody == null) {
            throw new Exception("No requestBody defined for this operation.");
        }

        return requestBody.getContent();
    }
}
