/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class NoOpStepTests extends OpenSearchTestCase {

    public void testNoOpStep() throws IOException {
        NoOpStep noopStep = new NoOpStep();
        CompletableFuture<WorkflowData> future = noopStep.execute(Collections.emptyList());
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
    }
}
