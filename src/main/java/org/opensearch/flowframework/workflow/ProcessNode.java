/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Representation of a process node in a workflow graph. Tracks predecessor nodes which must be completed before it can
 * start execution.
 */
public class ProcessNode {

    private static final Logger logger = LogManager.getLogger(ProcessNode.class);

    private final String id;
    private final WorkflowStep workflowStep;
    private final WorkflowData input;
    private final List<ProcessNode> predecessors;
    private final ThreadPool threadPool;
    private final TimeValue nodeTimeout;

    private final CompletableFuture<WorkflowData> future = new CompletableFuture<>();

    /**
     * Create this node linked to its executing process, including input data and any predecessor nodes.
     *
     * @param id           A string identifying the workflow step
     * @param workflowStep A java class implementing {@link WorkflowStep} to be executed when it's this node's turn.
     * @param input        Input required by the node encoded in a {@link WorkflowData} instance.
     * @param predecessors Nodes preceding this one in the workflow
     * @param threadPool   The OpenSearch thread pool
     * @param nodeTimeout  The timeout value for executing on this node
     */
    public ProcessNode(
        String id,
        WorkflowStep workflowStep,
        WorkflowData input,
        List<ProcessNode> predecessors,
        ThreadPool threadPool,
        TimeValue nodeTimeout
    ) {
        this.id = id;
        this.workflowStep = workflowStep;
        this.input = input;
        this.predecessors = predecessors;
        this.threadPool = threadPool;
        this.nodeTimeout = nodeTimeout;
    }

    /**
     * Returns the node's id.
     *
     * @return the node id.
     */
    public String id() {
        return id;
    }

    /**
     * Returns the node's workflow implementation.
     *
     * @return the workflow step
     */
    public WorkflowStep workflowStep() {
        return workflowStep;
    }

    /**
     * Returns the input data for this node.
     *
     * @return the input data
     */
    public WorkflowData input() {
        return input;
    }

    /**
     * Returns a {@link CompletableFuture} if this process is executing. Relies on the node having been sorted and
     * executed in an order such that all predecessor nodes have begun execution first (and thus populated this value).
     *
     * @return A future indicating the processing state of this node. Returns {@code null} if it has not begun
     *         executing, should not happen if a workflow is sorted and executed topologically.
     */
    public CompletableFuture<WorkflowData> future() {
        return future;
    }

    /**
     * Returns the predecessors of this node in the workflow. The predecessor's {@link #future()} must complete before
     * execution begins on this node.
     *
     * @return a set of predecessor nodes, if any. At least one node in the graph must have no predecessors and serve as
     *         a start node.
     */
    public List<ProcessNode> predecessors() {
        return predecessors;
    }

    /**
     * Execute this node in the sequence. Initializes the node's {@link CompletableFuture} and completes it when the
     * process completes.
     *
     * @return this node's future. This is returned immediately, while process execution continues asynchronously.
     */
    public CompletableFuture<WorkflowData> execute() {
        if (this.future.isDone()) {
            throw new IllegalStateException("Process Node [" + this.id + "] already executed.");
        }
        CompletableFuture.runAsync(() -> {
            List<CompletableFuture<WorkflowData>> predFutures = predecessors.stream().map(p -> p.future()).collect(Collectors.toList());
            predFutures.stream().forEach(CompletableFuture::join);
            logger.info(">>> Starting {}.", this.id);
            // get the input data from predecessor(s)
            List<WorkflowData> input = new ArrayList<WorkflowData>();
            input.add(this.input);
            for (CompletableFuture<WorkflowData> cf : predFutures) {
                try {
                    input.add(cf.get());
                } catch (InterruptedException | ExecutionException e) {
                    handleException(e);
                    return;
                }
            }
            try {
                ScheduledCancellable delayExec = null;
                if (this.nodeTimeout.compareTo(TimeValue.ZERO) > 0) {
                    delayExec = threadPool.schedule(() -> {
                        future.completeExceptionally(new TimeoutException("Execute timed out for " + this.id));
                        // If completed normally the above will be a no-op
                        if (future.isCompletedExceptionally()) {
                            logger.info(">>> Timed out {}.", this.id);
                        }
                    }, this.nodeTimeout, ThreadPool.Names.GENERIC);
                    // TODO: improve use of thread pool beyond generic
                    // https://github.com/opensearch-project/opensearch-ai-flow-framework/issues/61
                }
                CompletableFuture<WorkflowData> stepFuture = this.workflowStep.execute(input);
                future.complete(stepFuture.get()); // If completed exceptionally, this is a NOOP
                delayExec.cancel();
                logger.info(">>> Finished {}.", this.id);
            } catch (InterruptedException | ExecutionException e) {
                handleException(e);
            }
        }, threadPool.generic());
        return this.future;
    }

    private void handleException(Exception e) {
        // TODO: better handling of getCause
        this.future.completeExceptionally(e);
        logger.debug("<<< Completed Exceptionally {}", this.id, e.getCause());
    }

    @Override
    public String toString() {
        return this.id;
    }
}
