/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import static java.lang.Math.abs;
import static java.util.stream.Collectors.toList;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.flowframework.common.CommonValue.DESTINATION_INDEX;
import static org.opensearch.flowframework.common.CommonValue.SOURCE_INDICES;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.workflow.ReindexStep.NAME;
import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ReindexStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;
    private Client client;
    private ReindexStep reIndexStep;
    /** The refresh field for reindex */
    private static final String REFRESH = "refresh";
    /** The requests_per_second field for reindex */
    private static final String REQUESTS_PER_SECOND = "requests_per_second";
    /** The require_alias field for reindex */
    private static final String REQUIRE_ALIAS = "require_alias";
    /** The slices field for reindex */
    private static final String SLICES = "slices";
    /** The max_docs field for reindex */
    private static final String MAX_DOCS = "max_docs";

    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry(SOURCE_INDICES, "demo"),
                Map.entry(DESTINATION_INDEX, "dest"),
                Map.entry(REFRESH, true),
                Map.entry(REQUESTS_PER_SECOND, 2.0),
                Map.entry(REQUIRE_ALIAS, false),
                Map.entry(SLICES, 1),
                Map.entry(MAX_DOCS, 2)
            ),
            "test-id",
            "test-node-id"
        );

        client = mock(Client.class);
        reIndexStep = new ReindexStep(client, flowFrameworkIndicesHandler);
    }

    public void testReindexStep() throws ExecutionException, InterruptedException, IOException {

        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<BulkByScrollResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateResourceInStateIndex(anyString(), anyString(), anyString(), anyString(), any());

        PlainActionFuture<WorkflowData> future = reIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(client, times(1)).execute(any(), any(ReindexRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue()
            .onResponse(
                new BulkByScrollResponse(
                    timeValueMillis(randomNonNegativeLong()),
                    randomStatus(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    randomBoolean()
                )
            );

        assertTrue(future.isDone());

        Map<String, Object> outputData = Map.of(NAME, Map.of("demo", "dest"));
        assertEquals(outputData, future.get().getContent());

    }

    public void testReindexStepFailure() throws ExecutionException, InterruptedException {
        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<BulkByScrollResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = reIndexStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        assertFalse(future.isDone());
        verify(client, times(1)).execute(any(), any(ReindexRequest.class), actionListenerCaptor.capture());

        actionListenerCaptor.getValue().onFailure(new Exception("Failed to reindex from source demo to dest"));

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof Exception);
        assertEquals("Failed to reindex from source demo to dest", ex.getCause().getMessage());
    }

    private static BulkByScrollTask.Status randomStatus() {
        if (randomBoolean()) {
            return randomWorkingStatus(null);
        }
        boolean canHaveNullStatues = randomBoolean();
        List<BulkByScrollTask.StatusOrException> statuses = IntStream.range(0, between(0, 10)).mapToObj(i -> {
            if (canHaveNullStatues && LuceneTestCase.rarely()) {
                return null;
            }
            if (randomBoolean()) {
                return new BulkByScrollTask.StatusOrException(new OpenSearchException(randomAlphaOfLength(5)));
            }
            return new BulkByScrollTask.StatusOrException(randomWorkingStatus(i));
        }).collect(toList());
        return new BulkByScrollTask.Status(statuses, randomBoolean() ? "test" : null);
    }

    private static BulkByScrollTask.Status randomWorkingStatus(Integer sliceId) {
        // These all should be believably small because we sum them if we have multiple workers
        int total = between(0, 10000000);
        int updated = between(0, total);
        int created = between(0, total - updated);
        int deleted = between(0, total - updated - created);
        int noops = total - updated - created - deleted;
        int batches = between(0, 10000);
        long versionConflicts = between(0, total);
        long bulkRetries = between(0, 10000000);
        long searchRetries = between(0, 100000);
        // smallest unit of time during toXContent is Milliseconds
        TimeUnit[] timeUnits = { TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS };
        TimeValue throttled = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        TimeValue throttledUntil = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        return new BulkByScrollTask.Status(
            sliceId,
            total,
            updated,
            created,
            deleted,
            batches,
            versionConflicts,
            noops,
            bulkRetries,
            searchRetries,
            throttled,
            abs(Randomness.get().nextFloat()),
            randomBoolean() ? null : randomSimpleString(Randomness.get()),
            throttledUntil
        );
    }
}
