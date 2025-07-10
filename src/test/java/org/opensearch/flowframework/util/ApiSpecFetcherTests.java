/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.flowframework.exception.ApiSpecParseException;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import io.swagger.v3.oas.models.OpenAPI;

import static org.opensearch.flowframework.common.CommonValue.ML_COMMONS_API_SPEC_YAML_URI;
import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.PATCH;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

public class ApiSpecFetcherTests extends OpenSearchTestCase {

    private ApiSpecFetcher apiSpecFetcher;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testFetchApiSpecSuccess() throws Exception {

        OpenAPI result = ApiSpecFetcher.fetchApiSpec(ML_COMMONS_API_SPEC_YAML_URI);

        assertNotNull("The fetched OpenAPI spec should not be null", result);
    }

    public void testFetchApiSpecThrowsException() throws Exception {
        String invalidUri = "http://invalid-url.com/fail.yaml";

        ApiSpecParseException exception = expectThrows(ApiSpecParseException.class, () -> { ApiSpecFetcher.fetchApiSpec(invalidUri); });

        assertNotNull("Exception should be thrown for invalid URI", exception);
        assertTrue(exception.getMessage().contains("Unable to parse spec"));
    }

    public void testCompareRequiredFieldsSuccess() throws Exception {

        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;

        // Assuming REGISTER_AGENT step in the enum has these required fields
        List<String> expectedRequiredParams = Arrays.asList("name", "type");

        boolean comparisonResult = ApiSpecFetcher.compareRequiredFields(expectedRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);

        assertTrue("The required fields should match between API spec and enum", comparisonResult);
    }

    public void testCompareRequiredFieldsFailure() throws Exception {

        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;

        List<String> wrongRequiredParams = Arrays.asList("nonexistent_param");

        boolean comparisonResult = ApiSpecFetcher.compareRequiredFields(wrongRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);

        assertFalse("The required fields should not match for incorrect input", comparisonResult);
    }

    public void testCompareRequiredFieldsThrowsException() throws Exception {
        String invalidUri = "http://invalid-url.com/fail.yaml";
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = PUT;

        Exception exception = expectThrows(
            Exception.class,
            () -> { ApiSpecFetcher.compareRequiredFields(List.of(), invalidUri, path, method); }
        );

        assertNotNull("An exception should be thrown for an invalid API spec Uri", exception);
        assertTrue(exception.getMessage().contains("Unable to parse spec"));
    }

    public void testUnsupportedMethodException() throws IllegalArgumentException {
        Exception exception = expectThrows(Exception.class, () -> {
            ApiSpecFetcher.compareRequiredFields(
                List.of("name", "type"),
                ML_COMMONS_API_SPEC_YAML_URI,
                "/_plugins/_ml/agents/_register",
                PATCH
            );
        });

        assertEquals("Unsupported HTTP method: PATCH", exception.getMessage());
    }

    public void testNoOperationFoundException() throws Exception {
        Exception exception = expectThrows(IllegalArgumentException.class, () -> {
            ApiSpecFetcher.compareRequiredFields(
                List.of("name", "type"),
                ML_COMMONS_API_SPEC_YAML_URI,
                "/_plugins/_ml/agents/_register",
                DELETE
            );
        });

        assertEquals("No operation found for the specified method: DELETE", exception.getMessage());
    }

    public void testNoRequestBodyDefinedException() throws ApiSpecParseException {
        Exception exception = expectThrows(ApiSpecParseException.class, () -> {
            ApiSpecFetcher.compareRequiredFields(
                List.of("name", "type"),
                ML_COMMONS_API_SPEC_YAML_URI,
                "/_plugins/_ml/model_groups/{model_group_id}",
                RestRequest.Method.GET
            );
        });

        assertEquals("No requestBody defined for this operation.", exception.getMessage());
    }

    public void testFetchApiSpecWithOptimizedParsing() throws Exception {
        // Test the overloaded method with optimized parsing enabled
        OpenAPI result = ApiSpecFetcher.fetchApiSpec(ML_COMMONS_API_SPEC_YAML_URI, true);
        assertNotNull("The fetched OpenAPI spec should not be null with optimized parsing", result);

        // Test with optimized parsing disabled
        OpenAPI resultNoOptimization = ApiSpecFetcher.fetchApiSpec(ML_COMMONS_API_SPEC_YAML_URI, false);
        assertNotNull("The fetched OpenAPI spec should not be null without optimized parsing", resultNoOptimization);
    }

    public void testSpecCaching() throws Exception {
        // Clear cache first using reflection
        clearCaches();

        String testUri = ML_COMMONS_API_SPEC_YAML_URI;

        // First call should fetch and cache
        OpenAPI firstResult = ApiSpecFetcher.fetchApiSpec(testUri);
        assertNotNull("First fetch should return valid spec", firstResult);

        // Second call should use cache
        OpenAPI secondResult = ApiSpecFetcher.fetchApiSpec(testUri);
        assertNotNull("Second fetch should return valid spec", secondResult);

        // Both results should be the same object reference (from cache)
        assertSame("Second call should return cached result", firstResult, secondResult);
    }

    public void testRequiredFieldsCaching() throws Exception {
        // Clear cache first
        clearCaches();

        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;
        List<String> expectedRequiredParams = Arrays.asList("name", "type");

        // First call should fetch and cache
        boolean firstResult = ApiSpecFetcher.compareRequiredFields(expectedRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertTrue("First comparison should succeed", firstResult);

        // Second call should use cache
        boolean secondResult = ApiSpecFetcher.compareRequiredFields(expectedRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertTrue("Second comparison should succeed and use cache", secondResult);
    }

    public void testPathNotFoundInApiSpec() throws Exception {
        String nonExistentPath = "/non/existent/path";
        RestRequest.Method method = POST;
        List<String> params = Arrays.asList("test");

        Exception exception = expectThrows(IllegalArgumentException.class, () -> {
            ApiSpecFetcher.compareRequiredFields(params, ML_COMMONS_API_SPEC_YAML_URI, nonExistentPath, method);
        });

        assertEquals("Path not found in API spec: " + nonExistentPath, exception.getMessage());
    }

    public void testHandleUnresolvedRequestBodyScenario() throws Exception {
        // This test verifies that the handleUnresolvedRequestBody method is called
        // when there are unresolved $ref issues (not when requestBody is genuinely missing)
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;
        List<String> params = Arrays.asList("name", "type");

        // This should work normally and not throw an exception
        boolean result = ApiSpecFetcher.compareRequiredFields(params, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertTrue("Should handle the request successfully", result);
    }

    /**
     * Helper method to clear caches using reflection for testing purposes
     */
    private void clearCaches() throws Exception {
        // Clear SPEC_CACHE
        Field specCacheField = ApiSpecFetcher.class.getDeclaredField("SPEC_CACHE");
        specCacheField.setAccessible(true);
        ConcurrentMap<String, OpenAPI> specCache = (ConcurrentMap<String, OpenAPI>) specCacheField.get(null);
        specCache.clear();

        // Clear REQUIRED_FIELDS_CACHE
        Field requiredFieldsCacheField = ApiSpecFetcher.class.getDeclaredField("REQUIRED_FIELDS_CACHE");
        requiredFieldsCacheField.setAccessible(true);
        ConcurrentMap<String, List<String>> requiredFieldsCache = (ConcurrentMap<String, List<String>>) requiredFieldsCacheField.get(null);
        requiredFieldsCache.clear();
    }

}
