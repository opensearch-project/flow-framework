/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.exception;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

public class ApiSpecParseExceptionTests extends OpenSearchTestCase {

    public void testApiSpecParseException() {
        ApiSpecParseException exception = new ApiSpecParseException("API spec parsing failed");
        assertTrue(true);
        assertEquals("API spec parsing failed", exception.getMessage());
    }

    public void testApiSpecParseExceptionWithCause() {
        Throwable cause = new RuntimeException("Underlying issue");
        ApiSpecParseException exception = new ApiSpecParseException("API spec parsing failed", cause);
        assertTrue(true);
        assertEquals("API spec parsing failed", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    public void testApiSpecParseExceptionWithDetailedErrors() {
        String message = "API spec parsing failed";
        List<String> details = Arrays.asList("Missing required field", "Invalid type");

        ApiSpecParseException exception = new ApiSpecParseException(message, details);

        String expectedMessage = "API spec parsing failed: Missing required field, Invalid type";
        assertEquals(expectedMessage, exception.getMessage());
    }

}
