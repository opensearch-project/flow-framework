/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.GetWorkflowStateAction;
import org.opensearch.flowframework.transport.GetWorkflowStateRequest;
import org.opensearch.flowframework.transport.WorkflowResponse;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility class for managing timeout tasks in workflow execution.
 * This class provides methods to schedule timeout handlers, wrap listeners with timeout cancellation logic,
 * and fetch workflow states after timeouts.
 */
public class WorkflowTimeoutUtility {

    private static final Logger logger = LogManager.getLogger(WorkflowTimeoutUtility.class);
    private static final TimeValue MIN_TIMEOUT_MILLIS = TimeValue.timeValueSeconds(0);

    /**
     * Schedules a timeout task for a workflow execution.
     *
     * @param client        The OpenSearch client used to interact with the cluster.
     * @param threadPool    The thread pool to schedule the timeout task.
     * @param workflowId    The unique identifier of the workflow being executed.
     * @param listener      The listener to notify when the task completes or times out.
     * @param timeout       The timeout duration in milliseconds.
     * @param isResponseSent An atomic boolean to ensure the response is sent only once.
     * @return A wrapped ActionListener with timeout cancellation logic.
     */
    public static ActionListener<WorkflowResponse> scheduleTimeoutHandler(
        Client client,
        ThreadPool threadPool,
        final String workflowId,
        ActionListener<WorkflowResponse> listener,
        long timeout,
        AtomicBoolean isResponseSent
    ) {
        // Ensure timeout is within the valid range (non-negative)
        long adjustedTimeout = Math.max(timeout, MIN_TIMEOUT_MILLIS.millis());
        Scheduler.ScheduledCancellable scheduledCancellable = threadPool.schedule(
            new WorkflowTimeoutListener(client, workflowId, listener, isResponseSent),
            TimeValue.timeValueMillis(adjustedTimeout),
            ThreadPool.Names.GENERIC
        );

        return wrapWithTimeoutCancellationListener(listener, scheduledCancellable, isResponseSent);
    }

    /**
     * A listener that handles timeout for a workflow execution.
     */
    private static class WorkflowTimeoutListener implements Runnable {
        private final Client client;
        private final String workflowId;
        private final ActionListener<WorkflowResponse> listener;
        private final AtomicBoolean isResponseSent;

        WorkflowTimeoutListener(Client client, String workflowId, ActionListener<WorkflowResponse> listener, AtomicBoolean isResponseSent) {
            this.client = client;
            this.workflowId = workflowId;
            this.listener = listener;
            this.isResponseSent = isResponseSent;
        }

        @Override
        public void run() {
            if (isResponseSent.compareAndSet(false, true)) {
                logger.warn("Workflow execution timed out for workflowId: {}", workflowId);
                fetchWorkflowStateAfterTimeout(client, workflowId, listener);
            }
        }
    }

    /**
     * Wraps a listener with a timeout cancellation listener to cancel the timeout task when the workflow completes.
     *
     * @param listener             The original listener to wrap.
     * @param scheduledCancellable The cancellable timeout task.
     * @param isResponseSent        An atomic boolean to ensure the response is sent only once.
     * @param <Response>            The type of the response expected by the listener.
     * @return A wrapped ActionListener with timeout cancellation logic.
     */
    public static <Response> ActionListener<Response> wrapWithTimeoutCancellationListener(
        ActionListener<Response> listener,
        Scheduler.ScheduledCancellable scheduledCancellable,
        AtomicBoolean isResponseSent
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                if (isResponseSent.compareAndSet(false, true)) {
                    scheduledCancellable.cancel();
                }
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                if (isResponseSent.compareAndSet(false, true)) {
                    scheduledCancellable.cancel();
                }
                listener.onFailure(e);
            }
        };
    }

    /**
     * Handles the successful completion of a workflow.
     *
     * @param workflowId     The unique identifier of the workflow.
     * @param workflowResponse The response from the workflow execution.
     * @param isResponseSent  An atomic boolean to ensure the response is sent only once.
     * @param listener        The listener to notify of the workflow completion.
     */
    public static void handleResponse(
        String workflowId,
        WorkflowResponse workflowResponse,
        AtomicBoolean isResponseSent,
        ActionListener<WorkflowResponse> listener
    ) {
        if (isResponseSent.compareAndSet(false, true)) {
            listener.onResponse(new WorkflowResponse(workflowResponse.getWorkflowId(), workflowResponse.getWorkflowState()));
        } else {
            logger.info("Ignoring onResponse for workflowId: {} as timeout already occurred", workflowId);
        }
    }

    /**
     * Handles the failure of a workflow execution.
     *
     * @param workflowId    The unique identifier of the workflow.
     * @param e             The exception that occurred during workflow execution.
     * @param isResponseSent An atomic boolean to ensure the response is sent only once.
     * @param listener       The listener to notify of the workflow failure.
     */
    public static void handleFailure(
        String workflowId,
        Exception e,
        AtomicBoolean isResponseSent,
        ActionListener<WorkflowResponse> listener
    ) {
        if (isResponseSent.compareAndSet(false, true)) {
            FlowFrameworkException exception = new FlowFrameworkException(
                "Failed to execute workflow " + workflowId,
                ExceptionsHelper.status(e)
            );
            listener.onFailure(exception);
        } else {
            logger.info("Ignoring onFailure for workflowId: {} as timeout already occurred", workflowId);
        }
    }

    /**
     * Fetches the workflow state after a timeout has occurred.
     * This method sends a request to retrieve the current state of the workflow
     * and notifies the listener with the updated state or an error if the request fails.
     *
     * @param client      The OpenSearch client used to fetch the workflow state.
     * @param workflowId  The unique identifier of the workflow.
     * @param listener    The listener to notify with the updated state or failure.
     */
    public static void fetchWorkflowStateAfterTimeout(
        final Client client,
        final String workflowId,
        final ActionListener<WorkflowResponse> listener
    ) {
        client.execute(
            GetWorkflowStateAction.INSTANCE,
            new GetWorkflowStateRequest(workflowId, false),
            ActionListener.wrap(
                response -> listener.onResponse(new WorkflowResponse(workflowId, response.getWorkflowState())),
                exception -> listener.onFailure(
                    new FlowFrameworkException("Failed to get workflow state after timeout", ExceptionsHelper.status(exception))
                )
            )
        );
    }
}
