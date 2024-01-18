/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.UncategorizedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessNodeTests extends OpenSearchTestCase {

    private static TestThreadPool testThreadPool;
    private static ProcessNode successfulNode;
    private static ProcessNode failedNode;

    @BeforeClass
    public static void setup() {
        testThreadPool = new TestThreadPool(
            ProcessNodeTests.class.getName(),
            new ScalingExecutorBuilder(
                WORKFLOW_THREAD_POOL,
                1,
                OpenSearchExecutors.allocatedProcessors(Settings.EMPTY),
                TimeValue.timeValueMinutes(5),
                FLOW_FRAMEWORK_THREAD_POOL_PREFIX + WORKFLOW_THREAD_POOL
            )
        );

        PlainActionFuture<WorkflowData> successfulFuture = PlainActionFuture.newFuture();
        successfulFuture.onResponse(WorkflowData.EMPTY);
        PlainActionFuture<WorkflowData> failedFuture = PlainActionFuture.newFuture();
        failedFuture.onFailure(new RuntimeException("Test exception"));
        successfulNode = mock(ProcessNode.class);
        when(successfulNode.future()).thenReturn(successfulFuture);
        failedNode = mock(ProcessNode.class);
        when(failedNode.future()).thenReturn(failedFuture);
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testNode() throws InterruptedException, ExecutionException {
        // Tests where execute nas no timeout
        ProcessNode nodeA = new ProcessNode("A", new WorkflowStep() {
            @Override
            public PlainActionFuture<WorkflowData> execute(
                String currentNodeId,
                WorkflowData currentNodeInputs,
                Map<String, WorkflowData> outputs,
                Map<String, String> previousNodeInputs
            ) {
                PlainActionFuture<WorkflowData> f = PlainActionFuture.newFuture();
                f.onResponse(new WorkflowData(Map.of("test", "output"), "test-id", "test-node-id"));
                return f;
            }

            @Override
            public String getName() {
                return "test";
            }
        },
            Collections.emptyMap(),
            new WorkflowData(Map.of("test", "input"), Map.of("foo", "bar"), "test-id", "test-node-id"),
            List.of(successfulNode),
            testThreadPool,
            TimeValue.timeValueMillis(50)
        );
        assertEquals("A", nodeA.id());
        assertEquals("test", nodeA.workflowStep().getName());
        assertEquals("input", nodeA.input().getContent().get("test"));
        assertEquals("bar", nodeA.input().getParams().get("foo"));
        assertEquals("test-id", nodeA.input().getWorkflowId());
        assertEquals("test-node-id", nodeA.input().getNodeId());
        assertEquals(1, nodeA.predecessors().size());
        assertEquals(50, nodeA.nodeTimeout().millis());
        assertEquals("A", nodeA.toString());

        PlainActionFuture<WorkflowData> f = nodeA.execute();
        assertEquals(f, nodeA.future());
        assertEquals("output", f.get().getContent().get("test"));
    }

    public void testNodeNoTimeout() throws InterruptedException, ExecutionException {
        // Tests where execute finishes before timeout
        ProcessNode nodeB = new ProcessNode("B", new WorkflowStep() {
            @Override
            public PlainActionFuture<WorkflowData> execute(
                String currentNodeId,
                WorkflowData currentNodeInputs,
                Map<String, WorkflowData> outputs,
                Map<String, String> previousNodeInputs
            ) {
                PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();
                testThreadPool.schedule(() -> future.onResponse(WorkflowData.EMPTY), TimeValue.timeValueMillis(100), WORKFLOW_THREAD_POOL);
                return future;
            }

            @Override
            public String getName() {
                return "test";
            }
        }, Collections.emptyMap(), WorkflowData.EMPTY, Collections.emptyList(), testThreadPool, TimeValue.timeValueMillis(250));
        assertEquals("B", nodeB.id());
        assertEquals("test", nodeB.workflowStep().getName());
        assertEquals(WorkflowData.EMPTY, nodeB.input());
        assertEquals(Collections.emptyList(), nodeB.predecessors());
        assertEquals("B", nodeB.toString());

        PlainActionFuture<WorkflowData> f = nodeB.execute();
        assertEquals(f, nodeB.future());
        assertEquals(WorkflowData.EMPTY, f.get());
    }

    public void testNodeTimeout() throws InterruptedException, ExecutionException {
        // Tests where execute finishes after timeout
        ProcessNode nodeZ = new ProcessNode("Zzz", new WorkflowStep() {
            @Override
            public PlainActionFuture<WorkflowData> execute(
                String currentNodeId,
                WorkflowData currentNodeInputs,
                Map<String, WorkflowData> outputs,
                Map<String, String> previousNodeInputs
            ) {
                PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();
                testThreadPool.schedule(() -> future.onResponse(WorkflowData.EMPTY), TimeValue.timeValueMinutes(1), WORKFLOW_THREAD_POOL);
                return future;
            }

            @Override
            public String getName() {
                return "sleepy";
            }
        }, Collections.emptyMap(), WorkflowData.EMPTY, Collections.emptyList(), testThreadPool, TimeValue.timeValueMillis(100));
        assertEquals("Zzz", nodeZ.id());
        assertEquals("sleepy", nodeZ.workflowStep().getName());
        assertEquals(WorkflowData.EMPTY, nodeZ.input());
        assertEquals(Collections.emptyList(), nodeZ.predecessors());
        assertEquals("Zzz", nodeZ.toString());

        PlainActionFuture<WorkflowData> f = nodeZ.execute();
        UncategorizedExecutionException exception = assertThrows(UncategorizedExecutionException.class, () -> f.actionGet());
        assertTrue(f.isDone());
        assertEquals(ExecutionException.class, exception.getCause().getClass());
    }

    public void testExceptions() {
        // Tests where a predecessor future completed exceptionally
        ProcessNode nodeE = new ProcessNode("E", new WorkflowStep() {
            @Override
            public PlainActionFuture<WorkflowData> execute(
                String currentNodeId,
                WorkflowData currentNodeInputs,
                Map<String, WorkflowData> outputs,
                Map<String, String> previousNodeInputs
            ) {
                PlainActionFuture<WorkflowData> f = PlainActionFuture.newFuture();
                f.onResponse(WorkflowData.EMPTY);
                return f;
            }

            @Override
            public String getName() {
                return "test";
            }
        }, Collections.emptyMap(), WorkflowData.EMPTY, List.of(successfulNode, failedNode), testThreadPool, TimeValue.timeValueSeconds(15));
        assertEquals("E", nodeE.id());
        assertEquals("test", nodeE.workflowStep().getName());
        assertEquals(WorkflowData.EMPTY, nodeE.input());
        assertEquals(2, nodeE.predecessors().size());
        assertEquals("E", nodeE.toString());

        PlainActionFuture<WorkflowData> f = nodeE.execute();
        UncategorizedExecutionException exception = assertThrows(UncategorizedExecutionException.class, () -> f.actionGet());
        assertTrue(f.isDone());
        assertEquals(
            "java.util.concurrent.ExecutionException: java.lang.RuntimeException: Test exception",
            exception.getCause().getMessage()
        );

        // Tests where we already called execute
        assertThrows(IllegalStateException.class, () -> nodeE.execute());
    }
}
