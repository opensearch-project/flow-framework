/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.workflow.Workflow;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.template.TemplateTestJsonUtil.edge;
import static org.opensearch.flowframework.template.TemplateTestJsonUtil.node;
import static org.opensearch.flowframework.template.TemplateTestJsonUtil.workflow;

public class TemplateParserTests extends OpenSearchTestCase {

    private static final String MUST_HAVE_AT_LEAST_ONE_NODE = "A workflow must have at least one node.";
    private static final String NO_START_NODE_DETECTED = "No start node detected: all nodes have a predecessor.";
    private static final String CYCLE_DETECTED = "Cycle detected:";

    // Wrap parser into string list
    private static List<String> parse(String json) throws IOException {
        XContentParser parser = TemplateTestJsonUtil.jsonToParser(json);
        Workflow w = Workflow.parse(parser);
        return WorkflowProcessSorter.sortProcessNodes(w).stream().map(ProcessNode::id).collect(Collectors.toList());
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testOrdering() throws IOException {
        List<String> workflow;

        workflow = parse(workflow(List.of(node("A"), node("B"), node("C")), List.of(edge("C", "B"), edge("B", "A"))));
        assertEquals(0, workflow.indexOf("C"));
        assertEquals(1, workflow.indexOf("B"));
        assertEquals(2, workflow.indexOf("A"));

        workflow = parse(
            workflow(
                List.of(node("A"), node("B"), node("C"), node("D")),
                List.of(edge("A", "B"), edge("A", "C"), edge("B", "D"), edge("C", "D"))
            )
        );
        assertEquals(0, workflow.indexOf("A"));
        int b = workflow.indexOf("B");
        int c = workflow.indexOf("C");
        assertTrue(b == 1 || b == 2);
        assertTrue(c == 1 || c == 2);
        assertEquals(3, workflow.indexOf("D"));

        workflow = parse(
            workflow(
                List.of(node("A"), node("B"), node("C"), node("D"), node("E")),
                List.of(edge("A", "B"), edge("A", "C"), edge("B", "D"), edge("D", "E"), edge("C", "E"))
            )
        );
        assertEquals(0, workflow.indexOf("A"));
        b = workflow.indexOf("B");
        c = workflow.indexOf("C");
        int d = workflow.indexOf("D");
        assertTrue(b == 1 || b == 2);
        assertTrue(c == 1 || c == 2);
        assertTrue(d == 2 || d == 3);
        assertEquals(4, workflow.indexOf("E"));
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

    public void testNoEdges() throws IOException {
        List<String> workflow;
        Exception ex = assertThrows(IOException.class, () -> parse(workflow(Collections.emptyList(), Collections.emptyList())));
        assertEquals(MUST_HAVE_AT_LEAST_ONE_NODE, ex.getMessage());

        workflow = parse(workflow(List.of(node("A")), Collections.emptyList()));
        assertEquals(1, workflow.size());
        assertEquals("A", workflow.get(0));

        workflow = parse(workflow(List.of(node("A"), node("B")), Collections.emptyList()));
        assertEquals(2, workflow.size());
        assertTrue(workflow.contains("A"));
        assertTrue(workflow.contains("B"));
    }
}
