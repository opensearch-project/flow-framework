/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@ThreadLeakScope(Scope.NONE)
public class ProcessNodeTests extends OpenSearchTestCase {

    private static ExecutorService executor;

    @BeforeClass
    public static void setup() {
        executor = Executors.newFixedThreadPool(10);
    }

    @AfterClass
    public static void cleanup() {
        executor.shutdown();
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
        }, WorkflowData.EMPTY, Collections.emptyList(), executor);
        assertEquals("A", nodeA.id());
        assertEquals("test", nodeA.workflowStep().getName());
        assertEquals(WorkflowData.EMPTY, nodeA.input());
        assertEquals(Collections.emptyList(), nodeA.predecessors());
        assertEquals("A", nodeA.toString());

        // TODO: This test is flaky on Windows. Disabling until thread pool is integrated
        // https://github.com/opensearch-project/opensearch-ai-flow-framework/issues/42
        CompletableFuture<WorkflowData> f = nodeA.execute();
        assertEquals(f, nodeA.future());
        f.orTimeout(15, TimeUnit.SECONDS);
        assertTrue(f.isDone());
        assertEquals(WorkflowData.EMPTY, f.get());
    }
}
