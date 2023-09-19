/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.template.TemplateParser.DESTINATION;
import static org.opensearch.flowframework.template.TemplateParser.EDGES;
import static org.opensearch.flowframework.template.TemplateParser.NODES;
import static org.opensearch.flowframework.template.TemplateParser.NODE_ID;
import static org.opensearch.flowframework.template.TemplateParser.SOURCE;
import static org.opensearch.flowframework.template.TemplateParser.WORKFLOW;

public class TemplateParserTests extends OpenSearchTestCase {

    private static final String NO_START_NODE_DETECTED = "No start node detected: all nodes have a predecessor.";
    private static final String CYCLE_DETECTED = "Cycle detected:";

    // Input JSON generators
    private static String node(String id) {
        return "{\"" + NODE_ID + "\": \"" + id + "\"}";
    }

    private static String edge(String sourceId, String destId) {
        return "{\"" + SOURCE + "\": \"" + sourceId + "\", \"" + DESTINATION + "\": \"" + destId + "\"}";
    }

    private static String workflow(List<String> nodes, List<String> edges) {
        return "{\"" + WORKFLOW + "\": {" + arrayField(NODES, nodes) + ", " + arrayField(EDGES, edges) + "}}";
    }

    private static String arrayField(String fieldName, List<String> objects) {
        return "\"" + fieldName + "\": [" + objects.stream().collect(Collectors.joining(", ")) + "]";
    }

    // Output list elements
    private static ProcessNode expectedNode(String id) {
        return new ProcessNode(id, null, null);
    }

    // Less verbose parser
    private static List<ProcessNode> parse(String json) {
        return TemplateParser.parseJsonGraphToSequence(json);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testOrdering() {
        List<ProcessNode> workflow;

        workflow = parse(workflow(List.of(node("A"), node("B"), node("C")), List.of(edge("C", "B"), edge("B", "A"))));
        assertEquals(0, workflow.indexOf(expectedNode("C")));
        assertEquals(1, workflow.indexOf(expectedNode("B")));
        assertEquals(2, workflow.indexOf(expectedNode("A")));

        workflow = parse(
            workflow(
                List.of(node("A"), node("B"), node("C"), node("D")),
                List.of(edge("A", "B"), edge("A", "C"), edge("B", "D"), edge("C", "D"))
            )
        );
        assertEquals(0, workflow.indexOf(expectedNode("A")));
        int b = workflow.indexOf(expectedNode("B"));
        int c = workflow.indexOf(expectedNode("C"));
        assertTrue(b == 1 || b == 2);
        assertTrue(c == 1 || c == 2);
        assertEquals(3, workflow.indexOf(expectedNode("D")));

        workflow = parse(
            workflow(
                List.of(node("A"), node("B"), node("C"), node("D"), node("E")),
                List.of(edge("A", "B"), edge("A", "C"), edge("B", "D"), edge("D", "E"), edge("C", "E"))
            )
        );
        assertEquals(0, workflow.indexOf(expectedNode("A")));
        b = workflow.indexOf(expectedNode("B"));
        c = workflow.indexOf(expectedNode("C"));
        int d = workflow.indexOf(expectedNode("D"));
        assertTrue(b == 1 || b == 2);
        assertTrue(c == 1 || c == 2);
        assertTrue(d == 2 || d == 3);
        assertEquals(4, workflow.indexOf(expectedNode("E")));
    }

    public void testCycles() {
        Exception ex;

        ex = assertThrows(IllegalArgumentException.class, () -> parse(workflow(List.of(node("A")), List.of(edge("A", "A")))));
        assertEquals("Edge connects node A to itself.", ex.getMessage());

        ex = assertThrows(
            IllegalArgumentException.class,
            () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("A", "B"), edge("B", "B"))))
        );
        assertEquals("Edge connects node B to itself.", ex.getMessage());

        ex = assertThrows(
            IllegalArgumentException.class,
            () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("A", "B"), edge("B", "A"))))
        );
        assertEquals(NO_START_NODE_DETECTED, ex.getMessage());

        ex = assertThrows(
            IllegalArgumentException.class,
            () -> parse(workflow(List.of(node("A"), node("B"), node("C")), List.of(edge("A", "B"), edge("B", "C"), edge("C", "B"))))
        );
        assertTrue(ex.getMessage().startsWith(CYCLE_DETECTED));
        assertTrue(ex.getMessage().contains("B->C"));
        assertTrue(ex.getMessage().contains("C->B"));

        ex = assertThrows(
            IllegalArgumentException.class,
            () -> parse(
                workflow(
                    List.of(node("A"), node("B"), node("C"), node("D")),
                    List.of(edge("A", "B"), edge("B", "C"), edge("C", "D"), edge("D", "B"))
                )
            )
        );
        assertTrue(ex.getMessage().startsWith(CYCLE_DETECTED));
        assertTrue(ex.getMessage().contains("B->C"));
        assertTrue(ex.getMessage().contains("C->D"));
        assertTrue(ex.getMessage().contains("D->B"));
    }

    public void testNoEdges() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> parse(workflow(Collections.emptyList(), Collections.emptyList()))
        );
        assertEquals(NO_START_NODE_DETECTED, ex.getMessage());

        assertEquals(List.of(expectedNode("A")), parse(workflow(List.of(node("A")), Collections.emptyList())));

        List<ProcessNode> workflow = parse(workflow(List.of(node("A"), node("B")), Collections.emptyList()));
        assertEquals(2, workflow.size());
        assertTrue(workflow.contains(expectedNode("A")));
        assertTrue(workflow.contains(expectedNode("B")));
    }
}
