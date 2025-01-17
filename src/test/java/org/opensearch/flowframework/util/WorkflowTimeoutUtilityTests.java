/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.transport.GetWorkflowStateAction;
import org.opensearch.flowframework.transport.GetWorkflowStateRequest;
import org.opensearch.flowframework.transport.WorkflowResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WorkflowTimeoutUtilityTests extends OpenSearchTestCase {

    private Client mockClient;
    private ThreadPool mockThreadPool;
    private Scheduler.ScheduledCancellable mockScheduledCancellable;
    private AtomicBoolean isResponseSent;
    private ActionListener<WorkflowResponse> mockListener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClient = mock(Client.class);
        mockThreadPool = mock(ThreadPool.class);
        mockScheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        isResponseSent = new AtomicBoolean(false);
        mockListener = mock(ActionListener.class);

        when(mockThreadPool.schedule(any(Runnable.class), any(TimeValue.class), anyString())).thenReturn(mockScheduledCancellable);
    }

    public void testScheduleTimeoutHandler() {
        String workflowId = "testWorkflowId";
        long timeout = 1000L;

        ActionListener<WorkflowResponse> returnedListener = WorkflowTimeoutUtility.scheduleTimeoutHandler(
            mockClient,
            mockThreadPool,
            workflowId,
            null,
            mockListener,
            timeout,
            isResponseSent
        );

        assertNotNull(returnedListener);
        verify(mockThreadPool, times(1)).schedule(
            any(Runnable.class),
            eq(TimeValue.timeValueMillis(timeout)),
            eq(PROVISION_WORKFLOW_THREAD_POOL)
        );
    }

    public void testWrapWithTimeoutCancellationListenerOnResponse() {
        WorkflowResponse response = new WorkflowResponse(
            "testWorkflowId",
            new WorkflowState(
                "1",
                "test",
                "PROVISIONING",
                "IN_PROGRESS",
                Instant.now(),
                Instant.now(),
                TestHelpers.randomUser(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null
            )
        );
        Scheduler.ScheduledCancellable scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);

        ActionListener<WorkflowResponse> wrappedListener = WorkflowTimeoutUtility.wrapWithTimeoutCancellationListener(
            mockListener,
            scheduledCancellable,
            isResponseSent
        );

        wrappedListener.onResponse(response);

        assertTrue(isResponseSent.get());
        verify(scheduledCancellable, times(1)).cancel();
        verify(mockListener, times(1)).onResponse(response);
    }

    public void testWrapWithTimeoutCancellationListenerOnFailure() {
        Exception exception = new Exception("Test exception");
        Scheduler.ScheduledCancellable scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);

        ActionListener<WorkflowResponse> wrappedListener = WorkflowTimeoutUtility.wrapWithTimeoutCancellationListener(
            mockListener,
            scheduledCancellable,
            isResponseSent
        );

        wrappedListener.onFailure(exception);

        assertTrue(isResponseSent.get());
        verify(scheduledCancellable, times(1)).cancel();
        verify(mockListener, times(1)).onFailure(exception);
    }

    public void testFetchWorkflowStateAfterTimeout() {
        String workflowId = "testWorkflowId";
        ActionListener<WorkflowResponse> mockListener = mock(ActionListener.class);

        WorkflowTimeoutUtility.fetchWorkflowStateAfterTimeout(mockClient, workflowId, null, mockListener);

        verify(mockClient, times(1)).execute(
            eq(GetWorkflowStateAction.INSTANCE),
            any(GetWorkflowStateRequest.class),
            any(ActionListener.class)
        );
    }
}
