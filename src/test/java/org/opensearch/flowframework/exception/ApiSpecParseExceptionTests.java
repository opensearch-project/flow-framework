/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.exception;

import org.opensearch.OpenSearchException;
import org.opensearch.test.OpenSearchTestCase;

public class ApiSpecParseExceptionTests extends OpenSearchTestCase {

    public void testApiSpecParseException() {
        ApiSpecParseException exception = new ApiSpecParseException("API spec parsing failed");
        assertTrue(exception instanceof OpenSearchException);
        assertEquals("API spec parsing failed", exception.getMessage());
    }

    public void testApiSpecParseExceptionWithCause() {
        Throwable cause = new RuntimeException("Underlying issue");
        ApiSpecParseException exception = new ApiSpecParseException("API spec parsing failed", cause);
        assertTrue(exception instanceof OpenSearchException);
        assertEquals("API spec parsing failed", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    public void testApiSpecParseExceptionWithDetailedErrors() {
        ApiSpecParseException exception = new ApiSpecParseException("API spec parsing failed", "Missing required field");
        assertTrue(exception instanceof OpenSearchException);
        assertEquals("API spec parsing failed: Missing required field", exception.getMessage());
    }

}
