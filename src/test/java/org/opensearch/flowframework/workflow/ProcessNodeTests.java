/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ProcessNodeTests extends OpenSearchTestCase {

    private static TestThreadPool testThreadPool;

    @BeforeClass
    public static void setup() {
        testThreadPool = new TestThreadPool(ProcessNodeTests.class.getName());
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
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
        }, WorkflowData.EMPTY, Collections.emptyList(), testThreadPool, TimeValue.ZERO);
        assertEquals("A", nodeA.id());
        assertEquals("test", nodeA.workflowStep().getName());
        assertEquals(WorkflowData.EMPTY, nodeA.input());
        assertEquals(Collections.emptyList(), nodeA.predecessors());
        assertEquals("A", nodeA.toString());

        CompletableFuture<WorkflowData> f = nodeA.execute();
        assertEquals(f, nodeA.future());
        assertEquals(WorkflowData.EMPTY, f.get());
    }

    public void testNodeTimeout() throws InterruptedException, ExecutionException {
        ProcessNode nodeZ = new ProcessNode("Zzz", new WorkflowStep() {
            @Override
            public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
                CompletableFuture<WorkflowData> future = new CompletableFuture<>();
                testThreadPool.schedule(
                    () -> future.complete(WorkflowData.EMPTY),
                    TimeValue.timeValueMillis(250),
                    ThreadPool.Names.GENERIC
                );
                return future;
            }

            @Override
            public String getName() {
                return "sleepy";
            }
        }, WorkflowData.EMPTY, Collections.emptyList(), testThreadPool, TimeValue.timeValueMillis(100));
        assertEquals("Zzz", nodeZ.id());
        assertEquals("sleepy", nodeZ.workflowStep().getName());
        assertEquals(WorkflowData.EMPTY, nodeZ.input());
        assertEquals(Collections.emptyList(), nodeZ.predecessors());
        assertEquals("Zzz", nodeZ.toString());

        CompletableFuture<WorkflowData> f = nodeZ.execute();
        CompletionException exception = assertThrows(CompletionException.class, () -> f.join());
        assertTrue(f.isCompletedExceptionally());
        assertEquals(TimeoutException.class, exception.getCause().getClass());

        assertThrows(IllegalStateException.class, () -> nodeZ.execute());
    }
}
