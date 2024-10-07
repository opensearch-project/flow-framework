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
        RestRequest.Method method = RestRequest.Method.POST;

        // Assuming REGISTER_AGENT step in the enum has these required fields
        List<String> expectedRequiredParams = Arrays.asList("name", "type");

        boolean comparisonResult = ApiSpecFetcher.compareRequiredFields(expectedRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);

        assertTrue("The required fields should match between API spec and enum", comparisonResult);
    }

    public void testCompareRequiredFieldsFailure() throws Exception {

        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = RestRequest.Method.POST;

        List<String> wrongRequiredParams = Arrays.asList("nonexistent_param");

        boolean comparisonResult = ApiSpecFetcher.compareRequiredFields(wrongRequiredParams, ML_COMMONS_API_SPEC_YAML_URI, path, method);

        assertFalse("The required fields should not match for incorrect input", comparisonResult);
    }

    public void testCompareRequiredFieldsThrowsException() throws Exception {
        String invalidUri = "http://invalid-url.com/fail.yaml";
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = RestRequest.Method.POST;

        Exception exception = expectThrows(
            Exception.class,
            () -> { ApiSpecFetcher.compareRequiredFields(List.of(), invalidUri, path, method); }
        );

        assertNotNull("An exception should be thrown for an invalid API spec Uri", exception);
        assertTrue(exception.getMessage().contains("Unable to parse spec"));
    }

    public void testNoOperationFoundException() throws Exception {
        Exception exception = expectThrows(Exception.class, () -> {
            ApiSpecFetcher.compareRequiredFields(
                List.of("name", "type"),
                ML_COMMONS_API_SPEC_YAML_URI,
                "/invalid/path",
                RestRequest.Method.PATCH
            );
        });

        assertEquals("Unsupported HTTP method: PATCH", exception.getMessage());
    }

}
