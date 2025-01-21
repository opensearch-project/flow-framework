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
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.flowframework.common.CommonValue.DELAY_FIELD;

public class NoOpStepTests extends OpenSearchTestCase {

    public void testNoOpStep() throws IOException {
        NoOpStep noopStep = new NoOpStep();
        assertEquals(NoOpStep.NAME, noopStep.getName());
        PlainActionFuture<WorkflowData> future = noopStep.execute(
            "nodeId",
            WorkflowData.EMPTY,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
    }

    public void testNoOpStepDelay() throws IOException, InterruptedException {
        NoOpStep noopStep = new NoOpStep();
        WorkflowData delayData = new WorkflowData(Map.of(DELAY_FIELD, "1s"), null, null);

        long start = System.nanoTime();
        PlainActionFuture<WorkflowData> future = noopStep.execute(
            "nodeId",
            delayData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());
        // Sleep isn't exactly accurate so leave 100ms of roundoff
        assertTrue(System.nanoTime() - start > 900_000_000L);
    }

    public void testNoOpStepInterrupt() throws IOException, InterruptedException {
        NoOpStep noopStep = new NoOpStep();
        WorkflowData delayData = new WorkflowData(Map.of(DELAY_FIELD, "5s"), null, null);

        CountDownLatch latch = new CountDownLatch(1);
        // Fetch errors from the separate thread
        AtomicReference<AssertionError> assertionError = new AtomicReference<>();

        Thread testThread = new Thread(() -> {
            try {
                PlainActionFuture<WorkflowData> future = noopStep.execute(
                    "nodeId",
                    delayData,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    null
                );
                try {
                    future.actionGet();
                } catch (Exception e) {
                    // Ignore the IllegalStateExcption/InterruptedExcpetion
                }
                assertTrue(future.isDone());
                assertTrue(future.isCancelled());
                assertTrue(Thread.currentThread().isInterrupted());
            } catch (AssertionError e) {
                assertionError.set(e);
            } finally {
                latch.countDown();
            }
        });

        testThread.start();
        Thread.sleep(100);
        testThread.interrupt();

        latch.await(1, TimeUnit.SECONDS);

        if (assertionError.get() != null) {
            throw assertionError.get();
        }
    }

    public void testNoOpStepParse() throws IOException {
        NoOpStep noopStep = new NoOpStep();
        WorkflowData delayData = new WorkflowData(Map.of(DELAY_FIELD, "foo"), null, null);

        Exception ex = assertThrows(
            WorkflowStepException.class,
            () -> noopStep.execute("nodeId", delayData, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),null)
        );
        assertEquals("failed to parse setting [delay] with value [foo] as a time value: unit is missing or unrecognized", ex.getMessage());
    }
}
