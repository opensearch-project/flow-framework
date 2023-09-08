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

public class ProcessNode {
    private final String id;
    private CompletableFuture<String> future;

    // will be populated during graph parsing
    private Set<ProcessNode> predecessors = Collections.emptySet();

    ProcessNode(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public CompletableFuture<String> getFuture() {
        return future;
    }

    public Set<ProcessNode> getPredecessors() {
        return predecessors;
    }

    public void setPredecessors(Set<ProcessNode> predecessors) {
        this.predecessors = Set.copyOf(predecessors);
    }

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
            sleep(id.contains("ingest") ? 8000 : 4000);
            System.out.println("<<< Finished " + this.id);
            future.complete(this.id);
        });
        return this.future;
    }

    private void sleep(long i) {
        try {
            Thread.sleep(i);
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
