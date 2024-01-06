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

public class DeleteModelGroupTests extends OpenSearchTestCase {

    public void testDeleteModelGroup() throws IOException {
        DeleteModelGroupStep deleteModelGroupStep = new DeleteModelGroupStep();
        assertEquals(DeleteModelGroupStep.NAME, deleteModelGroupStep.getName());
        CompletableFuture<WorkflowData> future = deleteModelGroupStep.execute(
            "nodeId",
            WorkflowData.EMPTY,
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
    }
}
