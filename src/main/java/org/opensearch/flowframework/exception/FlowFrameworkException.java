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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Representation of Flow Framework Exceptions
 */
public class FlowFrameworkException extends OpenSearchException implements ToXContentObject {

    private static final long serialVersionUID = 1L;

    /** The rest status code of this exception */
    protected final RestStatus restStatus;

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

    /**
     * Read from a stream.
     * @param in THe input stream
     * @throws IOException on stream reading failure
     */
    public FlowFrameworkException(StreamInput in) throws IOException {
        super(in);
        restStatus = RestStatus.readFrom(in);
    }

    /**
     * Getter for restStatus.
     *
     * @return the HTTP status code associated with the exception
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }

    // Same getter but for superclass
    @Override
    public final RestStatus status() {
        return restStatus;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("error", this.getMessage()).endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        RestStatus.writeTo(out, restStatus);
    }

    // Keeping toXContentObject for backwards compatibility but this is needed for overriding superclass fragment
    @Override
    public boolean isFragment() {
        return false;
    }
}
