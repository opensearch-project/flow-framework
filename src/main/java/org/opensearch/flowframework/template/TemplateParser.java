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
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.workflow.Workflow;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;

import java.io.IOException;
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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

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
     * Parse a JSON use case template
     *
     * @param json A string containing a JSON representation of a use case template
     * @return A {@link Template} represented by the JSON.
     * @throws IOException on failure to parse
     */
    public static Template parseJsonToTemplate(String json) throws IOException {
        logger.info("Parsing template...");
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            json
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return Template.parse(parser);
    }

    /**
     * Parse a JSON representation of nodes and edges into a topologically sorted list of process nodes.
     * @param workflow A string containing a JSON representation of nodes and edges
     * @return A list of Process Nodes sorted topologically.  All predecessors of any node will occur prior to it in the list.
     */
    public static List<ProcessNode> parseWorkflowToSequence(Workflow workflow) {
        List<WorkflowNode> sortedNodes = topologicalSort(workflow.nodes(), workflow.edges());

        List<ProcessNode> nodes = new ArrayList<>();
        Map<String, ProcessNode> idToNodeMap = new HashMap<>();
        for (WorkflowNode node : sortedNodes) {
            WorkflowStep step = WorkflowStepFactory.get().createStep(node.type());
            WorkflowData data = new WorkflowData(node.inputs());
            List<ProcessNode> predecessorNodes = workflow.edges()
                .stream()
                .filter(e -> e.destination().equals(node.id()))
                // since we are iterating in topological order we know all predecessors will be in the map
                .map(e -> idToNodeMap.get(e.source()))
                .collect(Collectors.toList());

            ProcessNode processNode = new ProcessNode(node.id(), step, data, predecessorNodes);
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
