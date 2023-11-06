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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.TemplateTestJsonUtil;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        return workflowProcessSorter.sortProcessNodes(w, "123");
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
        MachineLearningNodeClient mlClient = mock(MachineLearningNodeClient.class);
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        when(client.admin()).thenReturn(adminClient);

        testThreadPool = new TestThreadPool(WorkflowProcessSorterTests.class.getName());
        WorkflowStepFactory factory = new WorkflowStepFactory(clusterService, client, mlClient, flowFrameworkIndicesHandler);
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

        ex = assertThrows(FlowFrameworkException.class, () -> parse(workflow(List.of(node("A")), List.of(edge("A", "A")))));
        assertEquals("Edge connects node A to itself.", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());

        ex = assertThrows(
            FlowFrameworkException.class,
            () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("A", "B"), edge("B", "B"))))
        );
        assertEquals("Edge connects node B to itself.", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());

        ex = assertThrows(
            FlowFrameworkException.class,
            () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("A", "B"), edge("B", "A"))))
        );
        assertEquals(NO_START_NODE_DETECTED, ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());

        ex = assertThrows(
            FlowFrameworkException.class,
            () -> parse(workflow(List.of(node("A"), node("B"), node("C")), List.of(edge("A", "B"), edge("B", "C"), edge("C", "B"))))
        );
        assertTrue(ex.getMessage().startsWith(CYCLE_DETECTED));
        assertTrue(ex.getMessage().contains("B->C"));
        assertTrue(ex.getMessage().contains("C->B"));
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());

        ex = assertThrows(
            FlowFrameworkException.class,
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
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());
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
            FlowFrameworkException.class,
            () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("C", "B"))))
        );
        assertEquals("Edge source C does not correspond to a node.", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());

        ex = assertThrows(FlowFrameworkException.class, () -> parse(workflow(List.of(node("A"), node("B")), List.of(edge("A", "C")))));
        assertEquals("Edge destination C does not correspond to a node.", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());

        ex = assertThrows(
            FlowFrameworkException.class,
            () -> parse(workflow(List.of(nodeWithType("A", "unimplemented_step")), Collections.emptyList()))
        );
        assertEquals("Workflow step type [unimplemented_step] is not implemented.", ex.getMessage());
        assertEquals(RestStatus.NOT_IMPLEMENTED, ((FlowFrameworkException) ex).getRestStatus());
    }

    public void testSuccessfulGraphValidation() throws Exception {
        WorkflowNode createConnector = new WorkflowNode(
            "workflow_step_1",
            CreateConnectorStep.NAME,
            Map.of(),
            Map.ofEntries(
                Map.entry("name", ""),
                Map.entry("description", ""),
                Map.entry("version", ""),
                Map.entry("protocol", ""),
                Map.entry("parameters", ""),
                Map.entry("credential", ""),
                Map.entry("actions", "")
            )
        );
        WorkflowNode registerModel = new WorkflowNode(
            "workflow_step_2",
            RegisterRemoteModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_1", "connector_id")),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            DeployModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_2", "model_id")),
            Map.of()
        );

        WorkflowEdge edge1 = new WorkflowEdge(createConnector.id(), registerModel.id());
        WorkflowEdge edge2 = new WorkflowEdge(registerModel.id(), deployModel.id());

        Workflow workflow = new Workflow(Map.of(), List.of(createConnector, registerModel, deployModel), List.of(edge1, edge2));

        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123");
        workflowProcessSorter.validateGraph(sortedProcessNodes);
    }

    public void testFailedGraphValidation() {

        // Create Register Model workflow node with missing connector_id field
        WorkflowNode registerModel = new WorkflowNode(
            "workflow_step_1",
            RegisterRemoteModelStep.NAME,
            Map.of(),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_2",
            DeployModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_1", "model_id")),
            Map.of()
        );
        WorkflowEdge edge = new WorkflowEdge(registerModel.id(), deployModel.id());
        Workflow workflow = new Workflow(Map.of(), List.of(registerModel, deployModel), List.of(edge));

        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123");
        FlowFrameworkException ex = expectThrows(
                FlowFrameworkException.class,
                () -> workflowProcessSorter.validateGraph(sortedProcessNodes)
        );
        assertEquals("Invalid graph, missing the following required inputs : [connector_id]", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ex.getRestStatus());
    }
}
