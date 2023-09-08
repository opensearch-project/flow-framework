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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TemplateParser {

    public static void main(String[] args) {
        String json = "{\n"
            + "    \"sequence\": {\n"
            + "        \"nodes\": [\n"
            + "            {\n"
            + "                \"id\": \"fetch_model\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"id\": \"create_ingest_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"id\": \"create_search_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"id\": \"create_neural_search_index\"\n"
            + "            }\n"
            + "        ],\n"
            + "        \"edges\": [\n"
            + "            {\n"
            + "                \"source\": \"fetch_model\",\n"
            + "                \"dest\": \"create_ingest_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"source\": \"fetch_model\",\n"
            + "                \"dest\": \"create_search_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"source\": \"create_ingest_pipeline\",\n"
            + "                \"dest\": \"create_neural_search_index\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"source\": \"create_search_pipeline\",\n"
            + "                \"dest\": \"create_neural_search_index\"\n"
            // + " }\n,"
            // + " {\n"
            // + " \"source\": \"create_neural_search_index\",\n"
            // + " \"dest\": \"fetch_model\"\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}";

        System.out.println(json);

        System.out.println("Parsing graph to sequence...");
        List<ProcessNode> processSequence = parseJsonGraphToSequence(json);
        List<CompletableFuture<String>> futureList = new ArrayList<>();

        for (ProcessNode n : processSequence) {
            Set<ProcessNode> predecessors = n.getPredecessors();
            System.out.format(
                "Queueing process [%s].  %s.%n",
                n.getId(),
                predecessors.isEmpty()
                    ? "Can start immediately!"
                    : String.format(
                        "Must wait for [%s] to complete first.",
                        predecessors.stream().map(p -> p.getId()).collect(Collectors.joining(", "))
                    )
            );
            futureList.add(n.execute());
        }
        futureList.forEach(CompletableFuture::join);
        System.out.println("All done!");
    }

    private static List<ProcessNode> parseJsonGraphToSequence(String json) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(json, JsonObject.class);

        JsonObject graph = jsonObject.getAsJsonObject("sequence");

        List<ProcessNode> nodes = new ArrayList<>();
        List<ProcessSequenceEdge> edges = new ArrayList<>();

        for (JsonElement nodeJson : graph.getAsJsonArray("nodes")) {
            JsonObject nodeObject = nodeJson.getAsJsonObject();
            String nodeId = nodeObject.get("id").getAsString();
            nodes.add(new ProcessNode(nodeId));
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
        Map<String, ProcessNode> nodeMap = nodes.stream().collect(Collectors.toMap(ProcessNode::getId, Function.identity()));
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
        System.out.println("Start node(s): " + sourceNodes);

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
        System.out.println("Execution sequence: " + sortedNodes);
        return sortedNodes;
    }
}
