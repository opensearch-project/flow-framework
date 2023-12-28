/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.TemplateTestJsonUtil;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_GET_TASK_REQUEST_RETRY;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOW_STEPS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.edge;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.node;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithType;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithTypeAndPreviousNodes;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithTypeAndTimeout;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.workflow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowProcessSorterTests extends OpenSearchTestCase {

    private static final String MUST_HAVE_AT_LEAST_ONE_NODE = "A workflow must have at least one node.";
    private static final String NO_START_NODE_DETECTED = "No start node detected: all nodes have a predecessor.";
    private static final String CYCLE_DETECTED = "Cycle detected:";

    // Wrap parser into workflow
    private static Workflow parseToWorkflow(String json) throws IOException {
        return Workflow.parse(TemplateTestJsonUtil.jsonToParser(json));
    }

    // Wrap parser into node list
    private static List<ProcessNode> parseToNodes(String json) throws IOException {
        return workflowProcessSorter.sortProcessNodes(parseToWorkflow(json), "123");
    }

    // Wrap parser into string list
    private static List<String> parse(String json) throws IOException {
        return parseToNodes(json).stream().map(ProcessNode::id).collect(Collectors.toList());
    }

    private static TestThreadPool testThreadPool;
    private static WorkflowProcessSorter workflowProcessSorter;
    private static Client client = mock(Client.class);
    private static ClusterService clusterService = mock(ClusterService.class);
    private static WorkflowValidator validator;

    @BeforeClass
    public static void setup() throws IOException {
        AdminClient adminClient = mock(AdminClient.class);
        MachineLearningNodeClient mlClient = mock(MachineLearningNodeClient.class);
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        Settings settings = Settings.builder().put("plugins.flow_framework.max_workflow_steps", 5).build();
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(FLOW_FRAMEWORK_ENABLED, MAX_WORKFLOWS, MAX_WORKFLOW_STEPS, WORKFLOW_REQUEST_TIMEOUT, MAX_GET_TASK_REQUEST_RETRY)
        ).collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        when(client.admin()).thenReturn(adminClient);

        testThreadPool = new TestThreadPool(WorkflowProcessSorterTests.class.getName());
        WorkflowStepFactory factory = new WorkflowStepFactory(
            Settings.EMPTY,
            clusterService,
            client,
            mlClient,
            flowFrameworkIndicesHandler
        );
        workflowProcessSorter = new WorkflowProcessSorter(factory, testThreadPool, clusterService, client, settings);
        validator = WorkflowValidator.parse("mappings/workflow-steps.json");
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

    public void testInferredEdges() throws IOException {
        Workflow w = parseToWorkflow(
            workflow(List.of(nodeWithTypeAndPreviousNodes("A", "noop"), nodeWithTypeAndPreviousNodes("B", "noop")), Collections.emptyList())
        );
        assertTrue(w.edges().isEmpty());

        w = parseToWorkflow(
            workflow(List.of(nodeWithTypeAndPreviousNodes("A", "noop"), nodeWithTypeAndPreviousNodes("B", "noop")), List.of(edge("B", "A")))
        );
        // edge from previous inputs only
        assertEquals(List.of(new WorkflowEdge("B", "A")), w.edges());

        w = parseToWorkflow(
            workflow(
                List.of(nodeWithTypeAndPreviousNodes("A", "noop", "B"), nodeWithTypeAndPreviousNodes("B", "noop")),
                Collections.emptyList()
            )
        );
        // edge from edges only
        assertEquals(List.of(new WorkflowEdge("B", "A")), w.edges());

        w = parseToWorkflow(
            workflow(
                List.of(
                    nodeWithTypeAndPreviousNodes("A", "noop", "B"),
                    nodeWithTypeAndPreviousNodes("B", "noop"),
                    nodeWithTypeAndPreviousNodes("C", "noop")
                ),
                List.of(edge("C", "A"))
            )
        );
        // combine sources, order not guaranteed
        assertEquals(2, w.edges().size());
        assertTrue(w.edges().contains(new WorkflowEdge("B", "A")));
        assertTrue(w.edges().contains(new WorkflowEdge("C", "A")));

        w = parseToWorkflow(
            workflow(
                List.of(
                    nodeWithTypeAndPreviousNodes("A", "noop", "B"),
                    nodeWithTypeAndPreviousNodes("B", "noop"),
                    nodeWithTypeAndPreviousNodes("C", "noop")
                ),
                List.of(edge("B", "A"))
            )
        );
        // duplicates, only 1
        assertEquals(List.of(new WorkflowEdge("B", "A")), w.edges());
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

        ex = assertThrows(FlowFrameworkException.class, () -> parse(workflow(List.of(node("A"), node("A")), Collections.emptyList())));
        assertEquals("Duplicate node id A.", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());

        ex = assertThrows(
            FlowFrameworkException.class,
            () -> parse(workflow(List.of(node("A"), node("B"), node("C"), node("D"), node("E"), node("F")), Collections.emptyList()))
        );
        String message = String.format(
            Locale.ROOT,
            "Workflow %s has %d nodes, which exceeds the maximum of %d. Change the setting [%s] to increase this.",
            "123",
            6,
            5,
            FlowFrameworkSettings.MAX_WORKFLOW_STEPS.getKey()
        );
        assertEquals(message, ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ((FlowFrameworkException) ex).getRestStatus());
    }

    public void testSuccessfulGraphValidation() throws Exception {
        WorkflowNode createConnector = new WorkflowNode(
            "workflow_step_1",
            CreateConnectorStep.NAME,
            Collections.emptyMap(),
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
            Map.ofEntries(Map.entry("workflow_step_1", CONNECTOR_ID)),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            DeployModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_2", MODEL_ID)),
            Collections.emptyMap()
        );

        WorkflowEdge edge1 = new WorkflowEdge(createConnector.id(), registerModel.id());
        WorkflowEdge edge2 = new WorkflowEdge(registerModel.id(), deployModel.id());

        Workflow workflow = new Workflow(
            Collections.emptyMap(),
            List.of(createConnector, registerModel, deployModel),
            List.of(edge1, edge2)
        );

        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123");
        workflowProcessSorter.validateGraph(sortedProcessNodes, validator);
    }

    public void testFailedGraphValidation() throws IOException {

        // Create Register Model workflow node with missing connector_id field
        WorkflowNode registerModel = new WorkflowNode(
            "workflow_step_1",
            RegisterRemoteModelStep.NAME,
            Collections.emptyMap(),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_2",
            DeployModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_1", MODEL_ID)),
            Collections.emptyMap()
        );
        WorkflowEdge edge = new WorkflowEdge(registerModel.id(), deployModel.id());
        Workflow workflow = new Workflow(Collections.emptyMap(), List.of(registerModel, deployModel), List.of(edge));

        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123");
        FlowFrameworkException ex = expectThrows(
            FlowFrameworkException.class,
            () -> workflowProcessSorter.validateGraph(sortedProcessNodes, validator)
        );
        assertEquals("Invalid workflow, node [workflow_step_1] missing the following required inputs : [connector_id]", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ex.getRestStatus());
    }

    public void testSuccessfulInstalledPluginValidation() throws Exception {

        // Mock and stub the cluster admin client to invoke the NodesInfoRequest
        AdminClient adminClient = mock(AdminClient.class);
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        // Mock and stub the clusterservice to get the local node
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.getNodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getLocalNodeId()).thenReturn("123");

        // Stub cluster admin client's node info request
        doAnswer(invocation -> {
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);

            // Mock and stub Plugin info
            PluginInfo mockedFlowPluginInfo = mock(PluginInfo.class);
            PluginInfo mockedMlPluginInfo = mock(PluginInfo.class);
            when(mockedFlowPluginInfo.getName()).thenReturn("opensearch-flow-framework");
            when(mockedMlPluginInfo.getName()).thenReturn("opensearch-ml");

            // Mock and stub PluginsAndModules
            PluginsAndModules mockedPluginsAndModules = mock(PluginsAndModules.class);
            when(mockedPluginsAndModules.getPluginInfos()).thenReturn(List.of(mockedFlowPluginInfo, mockedMlPluginInfo));

            // Mock and stub NodesInfoResponse to NodeInfo
            NodeInfo nodeInfo = mock(NodeInfo.class);
            @SuppressWarnings("unchecked")
            Map<String, NodeInfo> mockedMap = mock(Map.class);
            NodesInfoResponse response = mock(NodesInfoResponse.class);
            when(response.getNodesMap()).thenReturn(mockedMap);
            when(mockedMap.get(any())).thenReturn(nodeInfo);
            when(nodeInfo.getInfo(any())).thenReturn(mockedPluginsAndModules);

            // stub on response to pass the mocked NodesInfoRepsonse
            listener.onResponse(response);
            return null;

        }).when(clusterAdminClient).nodesInfo(any(NodesInfoRequest.class), any());

        WorkflowNode createConnector = new WorkflowNode(
            "workflow_step_1",
            CreateConnectorStep.NAME,
            Collections.emptyMap(),
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
            Map.ofEntries(Map.entry("workflow_step_1", CONNECTOR_ID)),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            DeployModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_2", MODEL_ID)),
            Collections.emptyMap()
        );

        WorkflowEdge edge1 = new WorkflowEdge(createConnector.id(), registerModel.id());
        WorkflowEdge edge2 = new WorkflowEdge(registerModel.id(), deployModel.id());

        Workflow workflow = new Workflow(
            Collections.emptyMap(),
            List.of(createConnector, registerModel, deployModel),
            List.of(edge1, edge2)
        );
        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123");

        workflowProcessSorter.validatePluginsInstalled(sortedProcessNodes, validator);
    }

    public void testFailedInstalledPluginValidation() throws Exception {

        // Mock and stub the cluster admin client to invoke the NodesInfoRequest
        AdminClient adminClient = mock(AdminClient.class);
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);

        // Mock and stub the clusterservice to get the local node
        ClusterState clusterState = mock(ClusterState.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.getNodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getLocalNodeId()).thenReturn("123");

        // Stub cluster admin client's node info request
        doAnswer(invocation -> {
            ActionListener<NodesInfoResponse> listener = invocation.getArgument(1);

            // Mock and stub Plugin info, We ommit the opensearch-ml info here to trigger validation failure
            PluginInfo mockedFlowPluginInfo = mock(PluginInfo.class);
            when(mockedFlowPluginInfo.getName()).thenReturn("opensearch-flow-framework");

            // Mock and stub PluginsAndModules
            PluginsAndModules mockedPluginsAndModules = mock(PluginsAndModules.class);
            when(mockedPluginsAndModules.getPluginInfos()).thenReturn(List.of(mockedFlowPluginInfo));

            // Mock and stub NodesInfoResponse to NodeInfo
            NodeInfo nodeInfo = mock(NodeInfo.class);
            @SuppressWarnings("unchecked")
            Map<String, NodeInfo> mockedMap = mock(Map.class);
            NodesInfoResponse response = mock(NodesInfoResponse.class);
            when(response.getNodesMap()).thenReturn(mockedMap);
            when(mockedMap.get(any())).thenReturn(nodeInfo);
            when(nodeInfo.getInfo(any())).thenReturn(mockedPluginsAndModules);

            // stub on response to pass the mocked NodesInfoRepsonse
            listener.onResponse(response);
            return null;

        }).when(clusterAdminClient).nodesInfo(any(NodesInfoRequest.class), any());

        WorkflowNode createConnector = new WorkflowNode(
            "workflow_step_1",
            CreateConnectorStep.NAME,
            Collections.emptyMap(),
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
            Map.ofEntries(Map.entry("workflow_step_1", CONNECTOR_ID)),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            DeployModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_2", MODEL_ID)),
            Collections.emptyMap()
        );

        WorkflowEdge edge1 = new WorkflowEdge(createConnector.id(), registerModel.id());
        WorkflowEdge edge2 = new WorkflowEdge(registerModel.id(), deployModel.id());

        Workflow workflow = new Workflow(
            Collections.emptyMap(),
            List.of(createConnector, registerModel, deployModel),
            List.of(edge1, edge2)
        );
        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123");

        FlowFrameworkException exception = expectThrows(
            FlowFrameworkException.class,
            () -> workflowProcessSorter.validatePluginsInstalled(sortedProcessNodes, validator)
        );

        assertEquals(
            "The workflowStep create_connector requires the following plugins to be installed : [opensearch-ml]",
            exception.getMessage()
        );
    }

    public void testReadWorkflowStepFile_withDefaultTimeout() throws IOException {
        // read timeout from node NODE_TIMEOUT_FIELD
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
                Map.entry("actions", ""),
                Map.entry("node_timeout", "50s")
            )
        );
        TimeValue createConnectorTimeout = workflowProcessSorter.parseTimeout(createConnector);
        assertEquals(50, createConnectorTimeout.getSeconds());

        // read timeout from workflow-step.json overwrite value
        WorkflowNode deployModel = new WorkflowNode(
            "workflow_step_3",
            DeployModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_2", MODEL_ID)),
            Map.of()
        );
        TimeValue deployModelTimeout = workflowProcessSorter.parseTimeout(deployModel);
        assertEquals(15, deployModelTimeout.getSeconds());

        // read timeout from NODE_TIMEOUT_DEFAULT_VALUE when there's no node NODE_TIMEOUT_FIELD
        // and no overwrite timeout value in workflow-step.json
        WorkflowNode registerModel = new WorkflowNode(
            "workflow_step_2",
            RegisterRemoteModelStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_1", CONNECTOR_ID)),
            Map.ofEntries(Map.entry("name", "name"), Map.entry("function_name", "remote"), Map.entry("description", "description"))
        );
        TimeValue registerRemoteModelTimeout = workflowProcessSorter.parseTimeout(registerModel);
        assertEquals(10, registerRemoteModelTimeout.getSeconds());
    }
}
