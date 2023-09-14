/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Representation of a process node in a workflow graph.  Tracks predecessor nodes which must be completed before it can start execution.
 */
public class ProcessNode {

    private static final Logger logger = LogManager.getLogger(ProcessNode.class);

    private final String id;
    private final WorkflowStep workflowStep;
    private CompletableFuture<WorkflowData> future = null;

    // will be populated during graph parsing
    private Set<ProcessNode> predecessors = Collections.emptySet();

    /**
     * Create this node linked to its executing process.
     *
     * @param id A string identifying the workflow step
     * @param workflowStep A java class implementing {@link WorkflowStep} to be executed when it's this node's turn.
     */
    ProcessNode(String id, WorkflowStep workflowStep) {
        this.id = id;
        this.workflowStep = workflowStep;
    }

    /**
     * Returns the node's id.
     * @return the node id.
     */
    public String id() {
        return id;
    }

    /**
     * Returns the node's workflow implementation.
     * @return the workflow step
     */
    public WorkflowStep workflowStep() {
        return workflowStep;
    }

    /**
     * Returns a {@link CompletableFuture} if this process is executing.
     * Relies on the node having been sorted and executed in an order such that all predecessor nodes have begun execution first (and thus populated this value).
     *
     * @return A future indicating the processing state of this node.
     * Returns {@code null} if it has not begun executing, should not happen if a workflow is sorted and executed topologically.
     */
    public CompletableFuture<WorkflowData> getFuture() {
        return future;
    }

    /**
     * Returns the predecessors of this node in the workflow.
     * The predecessor's {@link #getFuture()} must complete before execution begins on this node.
     *
     * @return a set of predecessor nodes, if any.  At least one node in the graph must have no predecessors and serve as a start node.
     */
    public Set<ProcessNode> getPredecessors() {
        return predecessors;
    }

    /**
     * Sets the predecessor node.  Called by {@link TemplateParser}.
     *
     * @param predecessors The predecessors of this node.
     */
    void setPredecessors(Set<ProcessNode> predecessors) {
        this.predecessors = Set.copyOf(predecessors);
    }

    /**
     * Execute this node in the sequence. Initializes the node's {@link CompletableFuture} and completes it when the process completes.
     *
     * @return this node's future. This is returned immediately, while process execution continues asynchronously.
     */
    public CompletableFuture<WorkflowData> execute(WorkflowData data) {
        this.future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            if (!predecessors.isEmpty()) {
                List<CompletableFuture<WorkflowData>> predFutures = predecessors.stream()
                    .map(p -> p.getFuture())
                    .collect(Collectors.toList());
                CompletableFuture<Void> waitForPredecessors = CompletableFuture.allOf(predFutures.toArray(new CompletableFuture<?>[0]));
                try {
                    waitForPredecessors.orTimeout(30, TimeUnit.SECONDS).get();
                } catch (InterruptedException | ExecutionException e) {
                    future.completeExceptionally(e);
                }
            }
            if (future.isCompletedExceptionally()) {
                return;
            }
            logger.debug(">>> Starting {}", this.id);
            CompletableFuture<WorkflowData> stepFuture = this.workflowStep.execute(data);
            stepFuture.join();
            try {
                future.complete(stepFuture.get());
                logger.debug("<<< Completed {}", this.id);
            } catch (InterruptedException | ExecutionException e) {
                // TODO: better handling of getCause
                future.completeExceptionally(e);
                logger.debug("<<< Completed Exceptionally {}", this.id);
            }
        });
        return this.future;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ProcessNode other = (ProcessNode) obj;
        return Objects.equals(id, other.id);
    }

    @Override
    public String toString() {
        return this.id;
    }
}
