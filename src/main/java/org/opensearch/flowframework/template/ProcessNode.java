/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

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
    private final String id;
    private CompletableFuture<String> future = null;

    // will be populated during graph parsing
    private Set<ProcessNode> predecessors = Collections.emptySet();

    /**
     * Create this node with a unique id.
     */
    ProcessNode(String id) {
        this.id = id;
    }

    /**
     * Returns the node's id.
     * @return the node id.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns a {@link CompletableFuture} if this process is executing.
     * Relies on the node having been sorted and executed in an order such that all predecessor nodes have begun execution first (and thus populated this value).
     *
     * @return A future indicating the processing state of this node.
     * Returns {@code null} if it has not begun executing, should not happen if a workflow is sorted and executed topologically.
     */
    public CompletableFuture<String> getFuture() {
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
    public CompletableFuture<String> execute() {
        this.future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            if (!predecessors.isEmpty()) {
                List<CompletableFuture<String>> predFutures = predecessors.stream().map(p -> p.getFuture()).collect(Collectors.toList());
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
            System.out.println(">>> Starting " + this.id);
            // TODO: Here is where we would call out to workflow step API
            workflowExecute(this.id);
            System.out.println("<<< Finished " + this.id);
            future.complete(this.id);
        });
        return this.future;
    }

    private void workflowExecute(String s) {
        try {
            Thread.sleep(s.contains("ingest") ? 8000 : 4000);
        } catch (InterruptedException e) {}
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
