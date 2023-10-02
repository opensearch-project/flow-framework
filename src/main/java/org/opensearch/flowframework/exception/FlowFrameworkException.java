/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.exception;

import javax.ws.rs.core.Response;

/**
 * Representation of Flow Framework Exceptions
 */
public class FlowFrameworkException extends RuntimeException {

    private final Response.Status restStatus;
    /**
     * Constructor with error message.
     *
     * @param message message of the exception
     * @param restStatus HTTP status code of the response
     */
    public FlowFrameworkException(String message, Response.Status restStatus) {
        super(message);
        this.restStatus = restStatus;
    }

    /**
     * Constructor with specified cause.
     * @param cause exception cause
     * @param restStatus HTTP status code of the response
     */
    public FlowFrameworkException(Throwable cause, Response.Status restStatus) {
        super(cause);
        this.restStatus = restStatus;
    }

    /**
     * Constructor with specified error message adn cause.
     * @param message error message
     * @param cause exception cause
     * @param restStatus HTTP status code of the response
     */
    public FlowFrameworkException(String message, Throwable cause, Response.Status restStatus) {
        super(message, cause);
        this.restStatus = restStatus;
    }
}
