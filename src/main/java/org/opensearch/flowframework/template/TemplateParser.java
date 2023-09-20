/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;

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

/**
 * Utility class for parsing templates.
 */
public class TemplateParser {

    private static final Logger logger = LogManager.getLogger(TemplateParser.class);

    // Field names in the JSON.
    // Currently package private for tests.
    // These may eventually become part of the template definition in which case they might be better declared public
    static final String WORKFLOW = "sequence";
    static final String NODES = "nodes";
    static final String NODE_ID = "id";
    static final String EDGES = "edges";
    static final String SOURCE = "source";
    static final String DESTINATION = "dest";
    static final String STEP_TYPE = "step_type";

    /**
     * Prevent instantiating this class.
     */
    private TemplateParser() {}

    /**
     * Parse a JSON representation of nodes and edges into a topologically sorted list of process nodes.
     * @param json A string containing a JSON representation of nodes and edges
     * @return A list of Process Nodes sorted topologically.  All predecessors of any node will occur prior to it in the list.
     */
    public static List<ProcessNode> parseJsonGraphToSequence(String json) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(json, JsonObject.class);

        JsonObject graph = jsonObject.getAsJsonObject(WORKFLOW);

        List<ProcessNode> nodes = new ArrayList<>();
        List<WorkflowEdge> edges = new ArrayList<>();

        for (JsonElement nodeJson : graph.getAsJsonArray(NODES)) {
            JsonObject nodeObject = nodeJson.getAsJsonObject();
            String nodeId = nodeObject.get(NODE_ID).getAsString();
            String stepType = nodeObject.get(STEP_TYPE).getAsString();
            WorkflowStep workflowStep = WorkflowStepFactory.get().createStep(stepType);
            WorkflowData inputData = WorkflowData.EMPTY;
            nodes.add(new ProcessNode(nodeId, workflowStep, inputData));
        }

        for (JsonElement edgeJson : graph.getAsJsonArray(EDGES)) {
            JsonObject edgeObject = edgeJson.getAsJsonObject();
            String sourceNodeId = edgeObject.get(SOURCE).getAsString();
            String destNodeId = edgeObject.get(DESTINATION).getAsString();
            if (sourceNodeId.equals(destNodeId)) {
                throw new IllegalArgumentException("Edge connects node " + sourceNodeId + " to itself.");
            }
            edges.add(new WorkflowEdge(sourceNodeId, destNodeId));
        }

        return topologicalSort(nodes, edges);
    }

    private static List<ProcessNode> topologicalSort(List<ProcessNode> nodes, List<WorkflowEdge> edges) {
        // Define the graph
        Set<WorkflowEdge> graph = new HashSet<>(edges);
        // Map node id string to object
        Map<String, ProcessNode> nodeMap = nodes.stream().collect(Collectors.toMap(ProcessNode::id, Function.identity()));
        // Build predecessor and successor maps
        Map<ProcessNode, Set<WorkflowEdge>> predecessorEdges = new HashMap<>();
        Map<ProcessNode, Set<WorkflowEdge>> successorEdges = new HashMap<>();
        for (WorkflowEdge edge : edges) {
            ProcessNode source = nodeMap.get(edge.getSource());
            ProcessNode dest = nodeMap.get(edge.getDestination());
            predecessorEdges.computeIfAbsent(dest, k -> new HashSet<>()).add(edge);
            successorEdges.computeIfAbsent(source, k -> new HashSet<>()).add(edge);
        }
        // update predecessors on the node object
        nodes.stream().filter(n -> predecessorEdges.containsKey(n)).forEach(n -> {
            n.setPredecessors(predecessorEdges.get(n).stream().map(e -> nodeMap.get(e.getSource())).collect(Collectors.toSet()));
        });

        // See https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
        // L <- Empty list that will contain the sorted elements
        List<ProcessNode> sortedNodes = new ArrayList<>();
        // S <- Set of all nodes with no incoming edge
        Queue<ProcessNode> sourceNodes = new ArrayDeque<>();
        nodes.stream().filter(n -> !predecessorEdges.containsKey(n)).forEach(n -> sourceNodes.add(n));
        if (sourceNodes.isEmpty()) {
            throw new IllegalArgumentException("No start node detected: all nodes have a predecessor.");
        }
        logger.debug("Start node(s): {}", sourceNodes);

        // while S is not empty do
        while (!sourceNodes.isEmpty()) {
            // remove a node n from S
            ProcessNode n = sourceNodes.poll();
            // add n to L
            sortedNodes.add(n);
            // for each node m with an edge e from n to m do
            for (WorkflowEdge e : successorEdges.getOrDefault(n, Collections.emptySet())) {
                ProcessNode m = nodeMap.get(e.getDestination());
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
