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

/**
 * Custom exception to be thrown when an error occurs during the parsing of an API specification.
 */
public class ApiSpecParseException extends OpenSearchException {

    /**
     * Constructor with message.
     *
     * @param message The detail message.
     */
    public ApiSpecParseException(String message) {
        super(message);
    }

    /**
     * Constructor with message and cause.
     *
     * @param message The detail message.
     * @param cause The cause of the exception.
     */
    public ApiSpecParseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor with message and list of detailed errors.
     *
     * @param message The detail message.
     * @param detailedErrors The list of errors encountered during the parsing process.
     */
    public ApiSpecParseException(String message, String detailedErrors) {
        super(message + ": " + detailedErrors);
    }
}
