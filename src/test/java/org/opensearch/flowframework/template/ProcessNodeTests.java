/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProcessNodeTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testNode() throws InterruptedException, ExecutionException {
        ProcessNode nodeA = new ProcessNode("A", new WorkflowStep() {
            @Override
            public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
                CompletableFuture<WorkflowData> f = new CompletableFuture<>();
                f.complete(WorkflowData.EMPTY);
                return f;
            }

            @Override
            public String getName() {
                return "test";
            }
        }, WorkflowData.EMPTY);
        assertEquals("A", nodeA.id());
        assertEquals("test", nodeA.workflowStep().getName());
        assertEquals(WorkflowData.EMPTY, nodeA.input());
        // FIXME: This is causing thread leaks
        CompletableFuture<WorkflowData> f = nodeA.execute();
        assertEquals(f, nodeA.getFuture());
        f.orTimeout(5, TimeUnit.SECONDS);
        assertTrue(f.isDone());
        assertEquals(WorkflowData.EMPTY, f.get());

        ProcessNode nodeB = new ProcessNode("B", null, null);
        assertNotEquals(nodeA, nodeB);

        ProcessNode nodeA2 = new ProcessNode("A", null, null);
        assertEquals(nodeA, nodeA2);
    }
}
