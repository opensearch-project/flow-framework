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

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import io.swagger.v3.oas.models.OpenAPI;

public class ApiSpecFetcherTests extends OpenSearchTestCase {

    private ApiSpecFetcher apiSpecFetcher;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.apiSpecFetcher = new ApiSpecFetcher(); // Initialize your fetcher
    }

    public void testFetchApiSpecSuccess() throws Exception {
        URI validUri = new URI(
            "https://raw.githubusercontent.com/junweid62/opensearch-api-specification/refs/heads/main/spec/namespaces/ml.yaml"
        );

        OpenAPI result = apiSpecFetcher.fetchApiSpec(validUri);

        assertNotNull("The fetched OpenAPI spec should not be null", result);
    }

    public void testFetchApiSpecThrowsException() throws Exception {
        URI invalidUri = new URI("http://invalid-url.com/fail.yaml");

        ApiSpecParseException exception = expectThrows(ApiSpecParseException.class, () -> { apiSpecFetcher.fetchApiSpec(invalidUri); });

        assertNotNull("Exception should be thrown for invalid URL", exception);
        assertTrue(exception.getMessage().contains("Unable to parse spec"));
    }

    public void testCompareRequiredFieldsSuccess() throws Exception {
        URI validUri = new URI(
            "https://raw.githubusercontent.com/junweid62/opensearch-api-specification/refs/heads/main/spec/namespaces/ml.yaml"
        );
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = RestRequest.Method.POST;

        // Assuming REGISTER_AGENT step in the enum has these required fields
        List<String> expectedRequiredParams = Arrays.asList("name", "type");

        boolean comparisonResult = apiSpecFetcher.compareRequiredFields(expectedRequiredParams, validUri, path, method);

        assertTrue("The required fields should match between API spec and enum", comparisonResult);
    }

    public void testCompareRequiredFieldsFailure() throws Exception {
        URI validUri = new URI(
            "https://raw.githubusercontent.com/junweid62/opensearch-api-specification/refs/heads/main/spec/namespaces/ml.yaml"
        );
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = RestRequest.Method.POST;

        List<String> wrongRequiredParams = Arrays.asList("nonexistent_param");

        boolean comparisonResult = apiSpecFetcher.compareRequiredFields(wrongRequiredParams, validUri, path, method);

        assertFalse("The required fields should not match for incorrect input", comparisonResult);
    }

    public void testCompareRequiredFieldsThrowsException() throws Exception {
        URI invalidUri = new URI("http://invalid-url.com/fail.yaml");
        String path = "/_plugins/_ml/agents/_register";
        RestRequest.Method method = RestRequest.Method.POST;

        Exception exception = expectThrows(
            Exception.class,
            () -> { apiSpecFetcher.compareRequiredFields(List.of(), invalidUri, path, method); }
        );

        assertNotNull("An exception should be thrown for an invalid API spec URI", exception);
        assertTrue(exception.getMessage().contains("Unable to parse spec"));
    }

}
