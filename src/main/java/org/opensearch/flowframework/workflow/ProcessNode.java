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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Representation of a process node in a workflow graph.
 * Tracks predecessor nodes which must be completed before it can start execution.
 */
public class ProcessNode {

    private static final Logger logger = LogManager.getLogger(ProcessNode.class);

    private final String id;
    private final WorkflowStep workflowStep;
    private final Map<String, String> previousNodeInputs;
    private final Map<String, String> params;
    private final WorkflowData input;
    private final List<ProcessNode> predecessors;
    private final ThreadPool threadPool;
    private final String threadPoolName;
    private final TimeValue nodeTimeout;
    private final String tenantId;

    private final PlainActionFuture<WorkflowData> future = PlainActionFuture.newFuture();

    /**
     * Create this node linked to its executing process, including input data and any predecessor nodes.
     *
     * @param id A string identifying the workflow step
     * @param workflowStep A java class implementing {@link WorkflowStep} to be executed when it's this node's turn.
     * @param previousNodeInputs A map of expected inputs coming from predecessor nodes used in graph validation
     * @param params Params passed on the REST path
     * @param input Input required by the node encoded in a {@link WorkflowData} instance.
     * @param predecessors Nodes preceding this one in the workflow
     * @param threadPool The OpenSearch thread pool
     * @param threadPoolName The thread pool to use
     * @param nodeTimeout The timeout value for executing on this node
     * @param tenantId The tenantId
     */
    public ProcessNode(
        String id,
        WorkflowStep workflowStep,
        Map<String, String> previousNodeInputs,
        Map<String, String> params,
        WorkflowData input,
        List<ProcessNode> predecessors,
        ThreadPool threadPool,
        String threadPoolName,
        TimeValue nodeTimeout,
        String tenantId
    ) {
        this.id = id;
        this.workflowStep = workflowStep;
        this.previousNodeInputs = previousNodeInputs;
        this.params = params;
        this.input = input;
        this.predecessors = predecessors;
        this.threadPool = threadPool;
        this.threadPoolName = threadPoolName;
        this.nodeTimeout = nodeTimeout;
        this.tenantId = tenantId;
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
     * Returns the node's expected predecessor node input
     * @return the expected predecessor node inputs
     */
    public Map<String, String> previousNodeInputs() {
        return previousNodeInputs;
    }

    /**
     * Returns the REST path params
     * @return the REST path params
     */
    public Map<String, String> params() {
        return params;
    }

    /**
     * Returns the input data for this node.
     * @return the input data
     */
    public WorkflowData input() {
        return input;
    }

    /**
     * Returns a {@link CompletableFuture} if this process is executing.
     * Relies on the node having been sorted and executed in an order such that all predecessor nodes have begun execution first (and thus populated this value).
     *
     * @return A future indicating the processing state of this node.
     * Returns {@code null} if it has not begun executing, should not happen if a workflow is sorted and executed topologically.
     */
    public PlainActionFuture<WorkflowData> future() {
        return future;
    }

    /**
     * Returns the predecessors of this node in the workflow.
     * The predecessor's {@link #future()} must complete before execution begins on this node.
     *
     * @return a set of predecessor nodes, if any.
     * At least one node in the graph must have no predecessors and serve as a start node.
     */
    public List<ProcessNode> predecessors() {
        return predecessors;
    }

    /**
     * Returns the timeout value of this node in the workflow. A value of {@link TimeValue#ZERO} means no timeout.
     * @return The node's timeout value.
     */
    public TimeValue nodeTimeout() {
        return nodeTimeout;
    }

    /**
     * Returns the tenantId value for this node in the workflow.
     * @return The node's tenantId value
     */
    public String tenantId() {
        return tenantId;
    }

    /**
     * Execute this node in the sequence.
     * Initializes the node's {@link CompletableFuture} and completes it when the process completes.
     *
     * @return this node's future.
     * This is returned immediately, while process execution continues asynchronously.
     */
    public PlainActionFuture<WorkflowData> execute() {
        if (this.future.isDone()) {
            throw new IllegalStateException("Process Node [" + this.id + "] already executed.");
        }

        CompletableFuture.runAsync(() -> {
            try {
                // get the input data from predecessor(s)
                Map<String, WorkflowData> inputMap = new HashMap<>();
                for (ProcessNode node : predecessors) {
                    WorkflowData wd = node.future().actionGet();
                    inputMap.put(wd.getNodeId(), wd);
                }

                // record start time for this step.
                logger.info("Starting {}.", this.id);
                PlainActionFuture<WorkflowData> stepFuture = this.workflowStep.execute(
                    this.id,
                    this.input,
                    inputMap,
                    this.previousNodeInputs,
                    this.params,
                    this.tenantId
                );
                // If completed exceptionally, this is a no-op
                future.onResponse(stepFuture.actionGet(this.nodeTimeout));
                // record end time passing workflow steps
                logger.info("Finished {}.", this.id);
            } catch (Exception e) {
                this.future.onFailure(e);
            }
        }, threadPool.executor(this.threadPoolName));
        return this.future;
    }

    @Override
    public String toString() {
        return this.id;
    }
}
