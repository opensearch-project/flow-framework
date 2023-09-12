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
import org.opensearch.flowframework.workflow.Workflow;

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

    /**
     * Prevent instantiating this class.
     */
    private TemplateParser() {}

    /**
     * Parse a JSON representation of nodes and edges into a topologically sorted list of process nodes.
     * @param json A string containing a JSON representation of nodes and edges
     * @param workflowSteps A map linking JSON node names to Java objects implementing {@link Workflow}
     * @return A list of Process Nodes sorted topologically.  All predecessors of any node will occur prior to it in the list.
     */
    public static List<ProcessNode> parseJsonGraphToSequence(String json, Map<String, Workflow> workflowSteps) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(json, JsonObject.class);

        JsonObject graph = jsonObject.getAsJsonObject("sequence");

        List<ProcessNode> nodes = new ArrayList<>();
        List<ProcessSequenceEdge> edges = new ArrayList<>();

        for (JsonElement nodeJson : graph.getAsJsonArray("nodes")) {
            JsonObject nodeObject = nodeJson.getAsJsonObject();
            String nodeId = nodeObject.get("id").getAsString();
            nodes.add(new ProcessNode(nodeId, workflowSteps.get(nodeId)));
        }

        for (JsonElement edgeJson : graph.getAsJsonArray("edges")) {
            JsonObject edgeObject = edgeJson.getAsJsonObject();
            String sourceNodeId = edgeObject.get("source").getAsString();
            String destNodeId = edgeObject.get("dest").getAsString();
            edges.add(new ProcessSequenceEdge(sourceNodeId, destNodeId));
        }

        return topologicalSort(nodes, edges);
    }

    private static List<ProcessNode> topologicalSort(List<ProcessNode> nodes, List<ProcessSequenceEdge> edges) {
        // Define the graph
        Set<ProcessSequenceEdge> graph = new HashSet<>(edges);
        // Map node id string to object
        Map<String, ProcessNode> nodeMap = nodes.stream().collect(Collectors.toMap(ProcessNode::id, Function.identity()));
        // Build predecessor and successor maps
        Map<ProcessNode, Set<ProcessSequenceEdge>> predecessorEdges = new HashMap<>();
        Map<ProcessNode, Set<ProcessSequenceEdge>> successorEdges = new HashMap<>();
        for (ProcessSequenceEdge edge : edges) {
            ProcessNode source = nodeMap.get(edge.getSource());
            ProcessNode dest = nodeMap.get(edge.getDestination());
            predecessorEdges.computeIfAbsent(dest, k -> new HashSet<>()).add(edge);
            successorEdges.computeIfAbsent(source, k -> new HashSet<>()).add(edge);
        }
        // See https://en.wikipedia.org/wiki/Topological_sorting#Kahn's_algorithm
        // Find start node(s) which have no predecessors
        Queue<ProcessNode> sourceNodes = new ArrayDeque<>();
        nodes.stream().filter(n -> !predecessorEdges.containsKey(n)).forEach(n -> sourceNodes.add(n));
        if (sourceNodes.isEmpty()) {
            throw new IllegalArgumentException("No start node detected: all nodes have a predecessor.");
        }
        logger.debug("Start node(s): {}", sourceNodes);

        // List to contain sorted elements
        List<ProcessNode> sortedNodes = new ArrayList<>();
        // Keep adding successors
        while (!sourceNodes.isEmpty()) {
            ProcessNode n = sourceNodes.poll();
            sortedNodes.add(n);
            if (predecessorEdges.containsKey(n)) {
                n.setPredecessors(predecessorEdges.get(n).stream().map(e -> nodeMap.get(e.getSource())).collect(Collectors.toSet()));
            }
            // Add successors to the queue
            for (ProcessSequenceEdge e : successorEdges.getOrDefault(n, Collections.emptySet())) {
                graph.remove(e);
                ProcessNode dest = nodeMap.get(e.getDestination());
                if (!sourceNodes.contains(dest) && !sortedNodes.contains(dest)) {
                    sourceNodes.add(dest);
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
