/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.TemplateTestJsonUtil;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.AdminClient;
import org.opensearch.transport.client.Client;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.CommonValue.DEPROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.PIPELINE_ID;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOW_STEPS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.TASK_REQUEST_RETRY_DURATION;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.edge;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.node;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithType;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithTypeAndPreviousNodes;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.nodeWithTypeAndTimeout;
import static org.opensearch.flowframework.model.TemplateTestJsonUtil.workflow;
import static org.junit.Assert.assertTrue;
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
        return workflowProcessSorter.sortProcessNodes(parseToWorkflow(json), "123", Collections.emptyMap(), null);
    }

    // Wrap parser into string list
    private static List<String> parse(String json) throws IOException {
        return parseToNodes(json).stream().map(ProcessNode::id).collect(Collectors.toList());
    }

    private static TestThreadPool testThreadPool;
    private static WorkflowProcessSorter workflowProcessSorter;
    private static Client client = mock(Client.class);
    private static ClusterService clusterService = mock(ClusterService.class);
    private static FlowFrameworkSettings flowFrameworkSettings;
    private static WorkflowStepFactory workflowStepFactory;

    private static Version templateVersion;
    private static List<Version> compatibilityVersions;
    private static Template reprovisionTemplate;
    private static ResourceCreated pipelineResource;
    private static ResourceCreated indexResource;

    @BeforeClass
    public static void setup() throws IOException {
        AdminClient adminClient = mock(AdminClient.class);
        MachineLearningNodeClient mlClient = mock(MachineLearningNodeClient.class);
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        Settings settings = Settings.builder().put("plugins.flow_framework.max_workflow_steps", 5).build();
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(FLOW_FRAMEWORK_ENABLED, MAX_WORKFLOWS, MAX_WORKFLOW_STEPS, WORKFLOW_REQUEST_TIMEOUT, TASK_REQUEST_RETRY_DURATION)
        ).collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);
        when(flowFrameworkSettings.getMaxWorkflowSteps()).thenReturn(5);

        when(client.admin()).thenReturn(adminClient);

        testThreadPool = new TestThreadPool(
            WorkflowProcessSorterTests.class.getName(),
            new ScalingExecutorBuilder(
                DEPROVISION_WORKFLOW_THREAD_POOL,
                1,
                Math.max(1, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
                TimeValue.timeValueMinutes(5),
                FLOW_FRAMEWORK_THREAD_POOL_PREFIX + DEPROVISION_WORKFLOW_THREAD_POOL
            )
        );
        workflowStepFactory = new WorkflowStepFactory(testThreadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings, client);
        workflowProcessSorter = new WorkflowProcessSorter(workflowStepFactory, testThreadPool, flowFrameworkSettings);

        templateVersion = Version.fromString("1.0.0");
        compatibilityVersions = List.of(Version.fromString("2.1.6"), Version.fromString("3.0.0"));

        // Register Search Pipeline Step
        String pipelineId = "pipelineId";
        String pipelineConfigurations =
            "{“description”:“An neural ingest pipeline”,“processors”:[{“text_embedding”:{“field_map”:{“text”:“analyzed_text”},“model_id”:“sdsadsadasd”}}]}";
        WorkflowNode createSearchPipeline = new WorkflowNode(
            "workflow_step_1",
            CreateSearchPipelineStep.NAME,
            Map.of(),
            Map.ofEntries(Map.entry(CONFIGURATIONS, pipelineConfigurations), Map.entry(PIPELINE_ID, pipelineId))
        );

        // Create Index Step
        String indexName = "indexName";
        String configurations =
            "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":2,\"number_of_replicas\":1,\"default_pipeline\":\"_none\",\"search\":{\"default_pipeline\":\"${{workflow_step_1.pipeline_id}}\"}}},\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}},\"aliases\":{\"sample-alias1\":{}}}";
        WorkflowNode createIndex = new WorkflowNode(
            "workflow_step_2",
            CreateIndexStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_1", PIPELINE_ID)),
            Map.ofEntries(Map.entry(INDEX_NAME, indexName), Map.entry(CONFIGURATIONS, configurations))
        );
        List<WorkflowNode> nodes = List.of(createSearchPipeline, createIndex);
        List<WorkflowEdge> edges = List.of(new WorkflowEdge("workflow_step_1", "workflow_step_2"));
        Workflow workflow = new Workflow(Map.of(), nodes, edges);
        Map<String, Object> uiMetadata = null;

        Instant now = Instant.now();
        reprovisionTemplate = new Template(
            "test",
            "a test template",
            "test use case",
            templateVersion,
            compatibilityVersions,
            Map.of("provision", workflow),
            uiMetadata,
            null,
            now,
            now,
            null,
            null
        );

        pipelineResource = new ResourceCreated(CreateSearchPipelineStep.NAME, "workflow_step_1", PIPELINE_ID, pipelineId);
        indexResource = new ResourceCreated(CreateIndexStep.NAME, "workflow_step_2", INDEX_NAME, indexName);
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
                    nodeWithType("default_timeout", "noop"),
                    nodeWithTypeAndTimeout("custom_timeout", "register_local_custom_model", "100ms")
                ),
                Collections.emptyList()
            )
        );
        ProcessNode node = workflow.get(0);
        assertEquals("default_timeout", node.id());
        assertEquals(NoOpStep.class, node.workflowStep().getClass());
        assertEquals(10, node.nodeTimeout().seconds());
        node = workflow.get(1);
        assertEquals("custom_timeout", node.id());
        assertEquals(RegisterLocalCustomModelStep.class, node.workflowStep().getClass());
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
        FlowFrameworkException ex = assertThrows(
            FlowFrameworkException.class,
            () -> parse(workflow(Collections.emptyList(), Collections.emptyList()))
        );
        assertEquals(MUST_HAVE_AT_LEAST_ONE_NODE, ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ex.getRestStatus());

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

        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123", Collections.emptyMap(), null);
        workflowProcessSorter.validateGraph(sortedProcessNodes);
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

        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123", Collections.emptyMap(), null);
        FlowFrameworkException ex = expectThrows(
            FlowFrameworkException.class,
            () -> workflowProcessSorter.validateGraph(sortedProcessNodes)
        );
        assertEquals("Invalid workflow, node [workflow_step_1] missing the following required inputs : [connector_id]", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ex.getRestStatus());
    }

    public void testFailedDenyListValidation() throws IOException {

        // Create Delete index workflow node
        WorkflowNode deleteIndex = new WorkflowNode(
            "workflow_step_1",
            DeleteIndexStep.NAME,
            Collections.emptyMap(),
            Map.of("index_name", "undeletable")
        );
        Workflow workflow = new Workflow(Collections.emptyMap(), List.of(deleteIndex), Collections.emptyList());

        FlowFrameworkException ex = expectThrows(
            FlowFrameworkException.class,
            () -> workflowProcessSorter.sortProcessNodes(workflow, "123", Collections.emptyMap(), null)
        );
        assertEquals("The step type [delete_index] for node [workflow_step_1] can not be used in a workflow.", ex.getMessage());
        assertEquals(RestStatus.FORBIDDEN, ex.getRestStatus());
    }

    public void testSuccessfulInstalledPluginValidation() throws Exception {

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
        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123", Collections.emptyMap(), null);

        workflowProcessSorter.validatePluginsInstalled(sortedProcessNodes, List.of("opensearch-flow-framework", "opensearch-ml"));
    }

    public void testFailedInstalledPluginValidation() throws Exception {

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
        List<ProcessNode> sortedProcessNodes = workflowProcessSorter.sortProcessNodes(workflow, "123", Collections.emptyMap(), null);

        FlowFrameworkException exception = expectThrows(
            FlowFrameworkException.class,
            () -> workflowProcessSorter.validatePluginsInstalled(sortedProcessNodes, List.of("opensearch-flow-framework"))
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

    public void testCreateReprovisionSequenceWithNoChange() {
        FlowFrameworkException ex = expectThrows(
            FlowFrameworkException.class,
            () -> workflowProcessSorter.createReprovisionSequence(
                "1",
                reprovisionTemplate,
                reprovisionTemplate,
                List.of(pipelineResource, indexResource),
                null
            )
        );

        assertEquals("Template does not contain any modifications", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ex.getRestStatus());

    }

    public void testCreateReprovisionSequenceWithDeletion() {
        // Register Search Pipeline Step
        String pipelineId = "pipelineId";
        String pipelineConfigurations =
            "{“description”:“An neural ingest pipeline”,“processors”:[{“text_embedding”:{“field_map”:{“text”:“analyzed_text”},“model_id”:“sdsadsadasd”}}]}";
        WorkflowNode createSearchPipeline = new WorkflowNode(
            "workflow_step_1",
            CreateSearchPipelineStep.NAME,
            Map.of(),
            Map.ofEntries(Map.entry(CONFIGURATIONS, pipelineConfigurations), Map.entry(PIPELINE_ID, pipelineId))
        );
        List<WorkflowNode> nodes = List.of(createSearchPipeline);
        Workflow workflow = new Workflow(Map.of(), nodes, List.of());
        Map<String, Object> uiMetadata = null;

        Instant now = Instant.now();
        Template templateWithNoCreateIndex = new Template(
            "test",
            "a test template",
            "test use case",
            templateVersion,
            compatibilityVersions,
            Map.of("provision", workflow),
            uiMetadata,
            null,
            now,
            now,
            null,
            null
        );

        FlowFrameworkException ex = expectThrows(
            FlowFrameworkException.class,
            () -> workflowProcessSorter.createReprovisionSequence(
                "1",
                reprovisionTemplate,
                templateWithNoCreateIndex,
                List.of(pipelineResource, indexResource),
                null
            )
        );

        assertEquals("Workflow Step deletion is not supported when reprovisioning a template.", ex.getMessage());
        assertEquals(RestStatus.BAD_REQUEST, ex.getRestStatus());

    }

    public void testCreateReprovisionSequenceWithAdditiveModification() throws Exception {

        // Register Search Pipeline Step
        String pipelineId = "pipelineId";
        String pipelineConfigurations =
            "{“description”:“An neural ingest pipeline”,“processors”:[{“text_embedding”:{“field_map”:{“text”:“analyzed_text”},“model_id”:“sdsadsadasd”}}]}";
        WorkflowNode createSearchPipeline = new WorkflowNode(
            "workflow_step_1",
            CreateSearchPipelineStep.NAME,
            Map.of(),
            Map.ofEntries(Map.entry(CONFIGURATIONS, pipelineConfigurations), Map.entry(PIPELINE_ID, pipelineId))
        );

        // Create Index Step
        String indexName = "indexName";
        String configurations =
            "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":2,\"number_of_replicas\":1,\"default_pipeline\":\"_none\",\"search\":{\"default_pipeline\":\"${{workflow_step_1.pipeline_id}}\"}}},\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}},\"aliases\":{\"sample-alias1\":{}}}";
        WorkflowNode createIndex = new WorkflowNode(
            "workflow_step_2",
            CreateIndexStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_1", PIPELINE_ID)),
            Map.ofEntries(Map.entry(INDEX_NAME, indexName), Map.entry(CONFIGURATIONS, configurations))
        );

        // Register ingest pipeline step
        String ingestPipelineId = "pipelineId";
        String ingestPipelineConfigurations =
            "{“description”:“An neural ingest pipeline”,“processors”:[{“text_embedding”:{“field_map”:{“text”:“analyzed_text”},“model_id”:“sdsadsadasd”}}]}";
        WorkflowNode createIngestPipeline = new WorkflowNode(
            "workflow_step_3",
            CreateIngestPipelineStep.NAME,
            Map.of(),
            Map.ofEntries(Map.entry(CONFIGURATIONS, ingestPipelineConfigurations), Map.entry(PIPELINE_ID, ingestPipelineId))
        );

        List<WorkflowNode> nodes = List.of(createSearchPipeline, createIndex, createIngestPipeline);
        List<WorkflowEdge> edges = List.of(new WorkflowEdge("workflow_step_1", "workflow_step_2"));
        Workflow workflow = new Workflow(Map.of(), nodes, edges);
        Map<String, Object> uiMetadata = null;

        Instant now = Instant.now();
        Template templateWithAdditiveModification = new Template(
            "test",
            "a test template",
            "test use case",
            templateVersion,
            compatibilityVersions,
            Map.of("provision", workflow),
            uiMetadata,
            null,
            now,
            now,
            null,
            null
        );

        List<ProcessNode> reprovisionSequence = workflowProcessSorter.createReprovisionSequence(
            "1",
            reprovisionTemplate,
            templateWithAdditiveModification,
            List.of(pipelineResource, indexResource),
            null
        );

        // Should result in a 3 step sequence
        assertTrue(reprovisionSequence.size() == 3);
        List<String> reprovisionWorkflowStepNames = reprovisionSequence.stream()
            .map(ProcessNode::workflowStep)
            .map(WorkflowStep::getName)
            .collect(Collectors.toList());
        // Assert 1 create ingest pipeline step in the sequence
        assertTrue(reprovisionWorkflowStepNames.contains(CreateIngestPipelineStep.NAME));
        // Assert 2 get resource steps in the sequence
        assertTrue(
            reprovisionWorkflowStepNames.stream().filter(x -> x.equals(WorkflowDataStep.NAME)).collect(Collectors.toList()).size() == 2
        );
    }

    public void testCreateReprovisionSequenceWithUpdates() throws Exception {
        // Register Search Pipeline Step with modified model ID
        String pipelineId = "pipelineId";
        String pipelineConfigurations =
            "{“description”:“An neural ingest pipeline”,“processors”:[{“text_embedding”:{“field_map”:{“text”:“analyzed_text”},“model_id”:“abcdefgg”}}]}";
        WorkflowNode createSearchPipeline = new WorkflowNode(
            "workflow_step_1",
            CreateSearchPipelineStep.NAME,
            Map.of(),
            Map.ofEntries(Map.entry(CONFIGURATIONS, pipelineConfigurations), Map.entry(PIPELINE_ID, pipelineId))
        );

        // Create Index Step with modifies index settings
        String indexName = "indexName";
        String configurations =
            "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":2,\"number_of_replicas\":1,\"default_pipeline\":\"test_pipeline_id\",\"search\":{\"default_pipeline\":\"${{workflow_step_1.pipeline_id}}\"}}},\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}},\"aliases\":{\"sample-alias1\":{}}}";
        WorkflowNode createIndex = new WorkflowNode(
            "workflow_step_2",
            CreateIndexStep.NAME,
            Map.ofEntries(Map.entry("workflow_step_1", PIPELINE_ID)),
            Map.ofEntries(Map.entry(INDEX_NAME, indexName), Map.entry(CONFIGURATIONS, configurations))
        );

        List<WorkflowNode> nodes = List.of(createSearchPipeline, createIndex);
        List<WorkflowEdge> edges = List.of(new WorkflowEdge("workflow_step_1", "workflow_step_2"));
        Workflow workflow = new Workflow(Map.of(), nodes, edges);
        Map<String, Object> uiMetadata = null;

        Instant now = Instant.now();
        Template templateWithModifiedNodes = new Template(
            "test",
            "a test template",
            "test use case",
            templateVersion,
            compatibilityVersions,
            Map.of("provision", workflow),
            uiMetadata,
            null,
            now,
            now,
            null,
            null
        );

        List<ProcessNode> reprovisionSequence = workflowProcessSorter.createReprovisionSequence(
            "1",
            reprovisionTemplate,
            templateWithModifiedNodes,
            List.of(pipelineResource, indexResource),
            null
        );

        // Should result in a 2 step sequence
        assertTrue(reprovisionSequence.size() == 2);
        List<String> reprovisionWorkflowStepNames = reprovisionSequence.stream()
            .map(ProcessNode::workflowStep)
            .map(WorkflowStep::getName)
            .collect(Collectors.toList());
        // Assert 1 update search pipeline step in the sequence
        assertTrue(reprovisionWorkflowStepNames.contains(UpdateSearchPipelineStep.NAME));
        // Assert update index step in the sequence
        assertTrue(reprovisionWorkflowStepNames.contains(UpdateIndexStep.NAME));
    }

}
