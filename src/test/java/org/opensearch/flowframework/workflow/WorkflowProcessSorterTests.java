/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.model.TemplateTestJsonUtil;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.model.TemplateTestJsonUtil.edge;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.node;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithType;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithTypeAndTimeout;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.workflow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowProcessSorterTests extends OpenSearchTestCase {

    private static final String MUST_HAVE_AT_LEAST_ONE_NODE = "A workflow must have at least one node.";
    private static final String NO_START_NODE_DETECTED = "No start node detected: all nodes have a predecessor.";
    private static final String CYCLE_DETECTED = "Cycle detected:";

    // Wrap parser into node list
    private static List<ProcessNode> parseToNodes(String json) throws IOException {
        XContentParser parser = TemplateTestJsonUtil.jsonToParser(json);
        Workflow w = Workflow.parse(parser);
        return workflowProcessSorter.sortProcessNodes(w);
    }

    // Wrap parser into string list
    private static List<String> parse(String json) throws IOException {
        return parseToNodes(json).stream().map(ProcessNode::id).collect(Collectors.toList());
    }

    private static TestThreadPool testThreadPool;
    private static WorkflowProcessSorter workflowProcessSorter;

    @BeforeClass
    public static void setup() {
        AdminClient adminClient = mock(AdminClient.class);
        ClusterService clusterService = mock(ClusterService.class);
        Client client = mock(Client.class);
        NodeClient nodeClient = mock(NodeClient.class);
        when(client.admin()).thenReturn(adminClient);

        testThreadPool = new TestThreadPool(WorkflowProcessSorterTests.class.getName());
        WorkflowStepFactory factory = new WorkflowStepFactory(clusterService, client, nodeClient);
        workflowProcessSorter = new WorkflowProcessSorter(factory, testThreadPool);
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testNodeDetails() throws IOException {
        List<ProcessNode> workflow = null;
        workflow = parseToNodes(
            workflow(
                List.of(
                    nodeWithType("default_timeout", "create_ingest_pipeline"),
                    nodeWithTypeAndTimeout("custom_timeout", "create_index", "100ms")
                ),
                Collections.emptyList()
            )
        );
        ProcessNode node = workflow.get(0);
        assertEquals("default_timeout", node.id());
        assertEquals(CreateIngestPipelineStep.class, node.workflowStep().getClass());
        assertEquals(10, node.nodeTimeout().seconds());
        node = workflow.get(1);
        assertEquals("custom_timeout", node.id());
        assertEquals(CreateIndexStep.class, node.workflowStep().getClass());
        assertEquals(100, node.nodeTimeout().millis());
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

    public void testExceptions() throws IOException {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("C", "B"))))
        );
        assertEquals("Edge source C does not correspond to a node.", ex.getMessage());

        ex = assertThrows(IllegalArgumentException.class, () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("A", "C")))));
        assertEquals("Edge destination C does not correspond to a node.", ex.getMessage());
    }
}
