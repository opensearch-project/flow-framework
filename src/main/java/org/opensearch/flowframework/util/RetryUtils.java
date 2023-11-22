/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

/**
 * Utility methods for retrying workflow step transport requests
 */
public class RetryUtils {

    /**
     * Utility method to indicate that a transport request can be retried
     *
     * @param retries the current retry count
     * @param maxRetry the maximum number of retries
     * @return boolean indicating whether or not to retry
     */
    public static boolean shouldRetry(int retries, int maxRetry) {
        return retries < maxRetry;
    }
}
