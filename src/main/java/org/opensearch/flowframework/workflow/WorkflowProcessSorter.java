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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.model.WorkflowNode.INPUTS_FIELD;
import static org.opensearch.flowframework.model.WorkflowNode.NODE_TIMEOUT_DEFAULT_VALUE;
import static org.opensearch.flowframework.model.WorkflowNode.NODE_TIMEOUT_FIELD;

/**
 * Utility class converting a workflow of nodes and edges into a topologically sorted list of Process Nodes.
 */
public class WorkflowProcessSorter {

    private static final Logger logger = LogManager.getLogger(WorkflowProcessSorter.class);

    private static WorkflowProcessSorter instance = null;

    private WorkflowStepFactory workflowStepFactory;
    private ThreadPool threadPool;

    /**
     * Create the singleton instance of this class. Throws an {@link IllegalStateException} if already created.
     *
     * @param workflowStepFactory The singleton instance of {@link WorkflowStepFactory}
     * @param threadPool          A thread executor
     * @return The created instance
     */
    public static synchronized WorkflowProcessSorter create(WorkflowStepFactory workflowStepFactory, ThreadPool threadPool) {
        if (instance != null) {
            throw new IllegalStateException("This class was already created.");
        }
        instance = new WorkflowProcessSorter(workflowStepFactory, threadPool);
        return instance;
    }

    /**
     * Gets the singleton instance of this class. Throws an {@link IllegalStateException} if not yet created.
     *
     * @return The created instance
     */
    public static synchronized WorkflowProcessSorter get() {
        if (instance == null) {
            throw new IllegalStateException("This factory has not yet been created.");
        }
        return instance;
    }

    private WorkflowProcessSorter(WorkflowStepFactory workflowStepFactory, ThreadPool threadPool) {
        this.workflowStepFactory = workflowStepFactory;
        this.threadPool = threadPool;
    }

    /**
     * Sort a workflow into a topologically sorted list of process nodes.
     * @param workflow A workflow with (unsorted) nodes and edges which define predecessors and successors
     * @return A list of Process Nodes sorted topologically.  All predecessors of any node will occur prior to it in the list.
     */
    public List<ProcessNode> sortProcessNodes(Workflow workflow) {
        List<WorkflowNode> sortedNodes = topologicalSort(workflow.nodes(), workflow.edges());

        List<ProcessNode> nodes = new ArrayList<>();
        Map<String, ProcessNode> idToNodeMap = new HashMap<>();
        for (WorkflowNode node : sortedNodes) {
            WorkflowStep step = workflowStepFactory.createStep(node.type());
            WorkflowData data = new WorkflowData(node.inputs(), workflow.userParams());
            List<ProcessNode> predecessorNodes = workflow.edges()
                .stream()
                .filter(e -> e.destination().equals(node.id()))
                // since we are iterating in topological order we know all predecessors will be in the map
                .map(e -> idToNodeMap.get(e.source()))
                .collect(Collectors.toList());

            TimeValue nodeTimeout = Setting.parseTimeValue(
                (String) node.inputs().getOrDefault(NODE_TIMEOUT_FIELD, NODE_TIMEOUT_DEFAULT_VALUE),
                TimeValue.ZERO,
                String.join(".", node.id(), INPUTS_FIELD, NODE_TIMEOUT_FIELD)
            );
            ProcessNode processNode = new ProcessNode(node.id(), step, data, predecessorNodes, threadPool, nodeTimeout);
            idToNodeMap.put(processNode.id(), processNode);
            nodes.add(processNode);
        }

        return nodes;
    }

    private static List<WorkflowNode> topologicalSort(List<WorkflowNode> workflowNodes, List<WorkflowEdge> workflowEdges) {
        // Basic validation
        Set<String> nodeIds = workflowNodes.stream().map(n -> n.id()).collect(Collectors.toSet());
        for (WorkflowEdge edge : workflowEdges) {
            String source = edge.source();
            if (!nodeIds.contains(source)) {
                throw new IllegalArgumentException("Edge source " + source + " does not correspond to a node.");
            }
            String dest = edge.destination();
            if (!nodeIds.contains(dest)) {
                throw new IllegalArgumentException("Edge destination " + dest + " does not correspond to a node.");
            }
            if (source.equals(dest)) {
                throw new IllegalArgumentException("Edge connects node " + source + " to itself.");
            }
        }

        // Build predecessor and successor maps
        Map<WorkflowNode, Set<WorkflowEdge>> predecessorEdges = new HashMap<>();
        Map<WorkflowNode, Set<WorkflowEdge>> successorEdges = new HashMap<>();
        Map<String, WorkflowNode> nodeMap = workflowNodes.stream().collect(Collectors.toMap(WorkflowNode::id, Function.identity()));
        for (WorkflowEdge edge : workflowEdges) {
            WorkflowNode source = nodeMap.get(edge.source());
            WorkflowNode dest = nodeMap.get(edge.destination());
            predecessorEdges.computeIfAbsent(dest, k -> new HashSet<>()).add(edge);
            successorEdges.computeIfAbsent(source, k -> new HashSet<>()).add(edge);
        }

        // See https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
        Set<WorkflowEdge> graph = new HashSet<>(workflowEdges);
        // L <- Empty list that will contain the sorted elements
        List<WorkflowNode> sortedNodes = new ArrayList<>();
        // S <- Set of all nodes with no incoming edge
        Queue<WorkflowNode> sourceNodes = new ArrayDeque<>();
        workflowNodes.stream().filter(n -> !predecessorEdges.containsKey(n)).forEach(n -> sourceNodes.add(n));
        if (sourceNodes.isEmpty()) {
            throw new IllegalArgumentException("No start node detected: all nodes have a predecessor.");
        }
        logger.debug("Start node(s): {}", sourceNodes);

        // while S is not empty do
        while (!sourceNodes.isEmpty()) {
            // remove a node n from S
            WorkflowNode n = sourceNodes.poll();
            // add n to L
            sortedNodes.add(n);
            // for each node m with an edge e from n to m do
            for (WorkflowEdge e : successorEdges.getOrDefault(n, Collections.emptySet())) {
                WorkflowNode m = nodeMap.get(e.destination());
                // remove edge e from the graph
                graph.remove(e);
                // if m has no other incoming edges then
                if (!predecessorEdges.get(m).stream().anyMatch(i -> graph.contains(i))) {
                    // insert m into S
                    sourceNodes.add(m);
                }
            }
        }
        if (!graph.isEmpty()) {
            throw new IllegalArgumentException("Cycle detected: " + graph);
        }
        logger.debug("Execution sequence: {}", sortedNodes);
        return sortedNodes;
    }
}
