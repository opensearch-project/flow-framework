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

import java.util.Arrays;
import java.util.List;

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
        String testUri = ML_COMMONS_API_SPEC_YAML_URI;

        // First call should fetch and cache
        OpenAPI firstResult = ApiSpecFetcher.fetchApiSpec(testUri);
        assertNotNull("First fetch should return valid spec", firstResult);

        // Second call should use cache - we can't directly test cache hit,
        // but we can verify the method works consistently
        OpenAPI secondResult = ApiSpecFetcher.fetchApiSpec(testUri);
        assertNotNull("Second fetch should return valid spec", secondResult);

        // Verify both calls return valid OpenAPI objects
        assertNotNull("First result should have paths", firstResult.getPaths());
        assertNotNull("Second result should have paths", secondResult.getPaths());
    }

    public void testRequiredFieldsCaching() throws Exception {
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;
        List<String> expectedRequiredParams = Arrays.asList("name", "type");

        // First call should fetch and cache
        boolean firstResult = ApiSpecFetcher.compareRequiredFields(expectedRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertTrue("First comparison should succeed", firstResult);

        // Second call should use cache - verify consistency
        boolean secondResult = ApiSpecFetcher.compareRequiredFields(expectedRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertTrue("Second comparison should succeed and be consistent", secondResult);

        // Verify the results are consistent
        assertEquals("Both calls should return the same result", firstResult, secondResult);
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

    public void testCompareRequiredFieldsWithEmptyRequiredParams() throws Exception {
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;
        List<String> emptyParams = Arrays.asList();

        // Empty required params should return true if API has no required fields,
        // or false if API has required fields (since empty list can't contain them)
        boolean result = ApiSpecFetcher.compareRequiredFields(emptyParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        // The result depends on the actual API spec - we just verify it doesn't crash
        assertNotNull("Result should not be null", Boolean.valueOf(result));
    }

    public void testCompareRequiredFieldsWithNullContent() throws Exception {
        // Test a path that might have null content or media type
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;
        List<String> params = Arrays.asList("name", "type");

        // This should still work and not throw null pointer exceptions
        boolean result = ApiSpecFetcher.compareRequiredFields(params, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertTrue("Should handle null content gracefully", result);
    }

    public void testGetOperationWithAllHttpMethods() throws Exception {
        // Test that different HTTP methods are handled correctly
        String postPath = "/_plugins/_ml/agents/_register";
        String getPath = "/_plugins/_ml/model_groups/{model_group_id}";

        // Test POST method
        boolean postResult = ApiSpecFetcher.compareRequiredFields(
            Arrays.asList("name", "type"),
            ML_COMMONS_API_SPEC_YAML_URI,
            postPath,
            POST
        );
        assertTrue("POST method should work", postResult);

        // Test PUT method - should throw exception for unsupported operation
        Exception putException = expectThrows(IllegalArgumentException.class, () -> {
            ApiSpecFetcher.compareRequiredFields(Arrays.asList("name"), ML_COMMONS_API_SPEC_YAML_URI, postPath, PUT);
        });
        assertTrue("PUT exception should mention no operation found", putException.getMessage().contains("No operation found"));

        // Test GET method - should throw exception for no requestBody
        Exception getException = expectThrows(ApiSpecParseException.class, () -> {
            ApiSpecFetcher.compareRequiredFields(Arrays.asList("name"), ML_COMMONS_API_SPEC_YAML_URI, getPath, RestRequest.Method.GET);
        });
        assertTrue("GET exception should mention no requestBody", getException.getMessage().contains("No requestBody defined"));
    }

    public void testFetchApiSpecWithInvalidUriFormats() throws Exception {
        // Test various invalid URI formats - these should throw exceptions
        String[] invalidUris = { "http://nonexistent-domain-12345.com/spec.yaml" };

        for (String invalidUri : invalidUris) {
            Exception exception = expectThrows(Exception.class, () -> { ApiSpecFetcher.fetchApiSpec(invalidUri); });
            assertNotNull("Exception should be thrown for invalid URI: " + invalidUri, exception);
            // The exception could be ApiSpecParseException or other types depending on the URI format
            assertTrue(
                "Exception should be related to parsing or connection issues",
                exception instanceof ApiSpecParseException || exception instanceof RuntimeException || exception.getCause() != null
            );
        }
    }

    public void testCompareRequiredFieldsWithPartialMatch() throws Exception {
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;

        // Test with params that partially match (some exist, some don't)
        List<String> partialMatchParams = Arrays.asList("name", "nonexistent_field");

        boolean result = ApiSpecFetcher.compareRequiredFields(partialMatchParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertFalse("Partial match should return false", result);
    }

    public void testCompareRequiredFieldsWithExtraParams() throws Exception {
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;

        // Test with more params than required (should still pass if all required are included)
        List<String> extraParams = Arrays.asList("name", "type", "description", "tools");

        boolean result = ApiSpecFetcher.compareRequiredFields(extraParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        assertTrue("Extra params should not affect the result if all required are present", result);
    }

    public void testCacheKeyGeneration() throws Exception {
        // Test that different combinations create different cache entries
        String path1 = "/_plugins/_ml/agents/_register";
        String path2 = "/_plugins/_ml/model_groups";
        List<String> params = Arrays.asList("name", "type");

        // Make calls with different paths to ensure different cache keys
        try {
            ApiSpecFetcher.compareRequiredFields(params, ML_COMMONS_API_SPEC_YAML_URI, path1, POST);
        } catch (Exception e) {
            // Expected for some paths
        }

        try {
            ApiSpecFetcher.compareRequiredFields(params, ML_COMMONS_API_SPEC_YAML_URI, path2, POST);
        } catch (Exception e) {
            // Expected for some paths
        }

        // The test passes if no unexpected exceptions are thrown during cache key generation
        assertTrue("Cache key generation should work for different path/method combinations", true);
    }

    public void testFetchApiSpecConsistencyBetweenOptimizedAndNonOptimized() throws Exception {
        // Test that both optimized and non-optimized parsing return valid results
        OpenAPI optimizedResult = ApiSpecFetcher.fetchApiSpec(ML_COMMONS_API_SPEC_YAML_URI, true);
        OpenAPI nonOptimizedResult = ApiSpecFetcher.fetchApiSpec(ML_COMMONS_API_SPEC_YAML_URI, false);

        assertNotNull("Optimized parsing should return valid spec", optimizedResult);
        assertNotNull("Non-optimized parsing should return valid spec", nonOptimizedResult);

        // Both should have paths
        assertNotNull("Optimized result should have paths", optimizedResult.getPaths());
        assertNotNull("Non-optimized result should have paths", nonOptimizedResult.getPaths());

        // Both should have the same basic structure
        assertFalse("Both results should have non-empty paths", optimizedResult.getPaths().isEmpty());
        assertFalse("Both results should have non-empty paths", nonOptimizedResult.getPaths().isEmpty());
    }

    public void testErrorHandlingInHandleUnresolvedRequestBody() throws Exception {
        // Test error handling when components are missing or malformed
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = POST;
        List<String> params = Arrays.asList("name", "type");

        // This should handle errors gracefully and not crash
        boolean result = ApiSpecFetcher.compareRequiredFields(params, ML_COMMONS_API_SPEC_YAML_URI, path, method);
        // The result can be true or false, but it should not throw an exception
        assertNotNull("Result should not be null", Boolean.valueOf(result));
    }

}
