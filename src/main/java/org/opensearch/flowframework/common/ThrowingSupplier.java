/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

/**
 * A supplier that can throw checked exception
 *
 * @param <T> method parameter type
 * @param <E> Exception type
 */
@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
    /**
     * Gets a result or throws an exception if unable to produce a result.
     *
     * @return the result
     * @throws E if unable to produce a result
     */
    T get() throws E;
}
