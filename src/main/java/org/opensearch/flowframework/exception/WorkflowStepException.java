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
import org.opensearch.OpenSearchParseException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Representation of an exception that is caused by a workflow step failing outside of our plugin
 * This is caught by an external client (e.g. ml-client) returning the failure
 */
public class WorkflowStepException extends FlowFrameworkException implements ToXContentObject {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor with error message.
     *
     * @param message message of the exception
     * @param restStatus HTTP status code of the response
     */
    public WorkflowStepException(String message, RestStatus restStatus) {
        super(message, restStatus);
    }

    /**
     * Constructor with specified cause.
     * @param cause exception cause
     * @param restStatus HTTP status code of the response
     */
    public WorkflowStepException(Throwable cause, RestStatus restStatus) {
        super(cause, restStatus);
    }

    /**
     * Constructor with specified error message adn cause.
     * @param message error message
     * @param cause exception cause
     * @param restStatus HTTP status code of the response
     */
    public WorkflowStepException(String message, Throwable cause, RestStatus restStatus) {
        super(message, cause, restStatus);
    }

    /**
     * Getter for restStatus.
     *
     * @return the HTTP status code associated with the exception
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("error", this.getMessage()).endObject();
    }

    /**
     * Getter for safe exceptions
     * @param ex exception
     * @return exception if safe
     */
    public static Exception getSafeException(Exception ex) {
        if (ex instanceof IllegalArgumentException
            || ex instanceof OpenSearchStatusException
            || ex instanceof OpenSearchParseException
            || (ex instanceof OpenSearchException && ex.getCause() instanceof OpenSearchParseException)) {
            return ex;
        }
        return null;
    }
}
