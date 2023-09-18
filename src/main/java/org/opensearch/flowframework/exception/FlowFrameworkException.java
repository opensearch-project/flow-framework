/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.exception;

public class FlowFrameworkException extends RuntimeException {
    /**
     * Constructor with error message.
     *
     * @param message message of the exception
     */
    public FlowFrameworkException(String message) {
        super(message);
    }
}
