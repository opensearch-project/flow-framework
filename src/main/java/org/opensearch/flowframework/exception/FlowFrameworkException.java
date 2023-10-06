/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.exception;

import org.opensearch.core.rest.RestStatus;

/**
 * Representation of Flow Framework Exceptions
 */
public class FlowFrameworkException extends RuntimeException {

    private final RestStatus restStatus;

    /**
     * Constructor with error message.
     *
     * @param message message of the exception
     * @param restStatus HTTP status code of the response
     */
    public FlowFrameworkException(String message, RestStatus restStatus) {
        super(message);
        this.restStatus = restStatus;
    }

    /**
     * Constructor with specified cause.
     * @param cause exception cause
     * @param restStatus HTTP status code of the response
     */
    public FlowFrameworkException(Throwable cause, RestStatus restStatus) {
        super(cause);
        this.restStatus = restStatus;
    }

    /**
     * Constructor with specified error message adn cause.
     * @param message error message
     * @param cause exception cause
     * @param restStatus HTTP status code of the response
     */
    public FlowFrameworkException(String message, Throwable cause, RestStatus restStatus) {
        super(message, cause);
        this.restStatus = restStatus;
    }
}
