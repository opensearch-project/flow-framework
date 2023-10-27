/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

/**
 * Enum relating to the state of a workflow
 */
public enum State {
    NOT_STARTED,
    PROVISIONING,
    FAILED,
    READY
}
