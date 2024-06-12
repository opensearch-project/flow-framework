/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginsService;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOW_STEPS;
import static org.opensearch.flowframework.model.WorkflowNode.NODE_TIMEOUT_DEFAULT_VALUE;
import static org.opensearch.flowframework.model.WorkflowNode.NODE_TIMEOUT_FIELD;
import static org.opensearch.flowframework.model.WorkflowNode.USER_INPUTS_FIELD;
import static org.opensearch.flowframework.workflow.WorkflowStepFactory.WorkflowSteps.getInputByWorkflowType;
import static org.opensearch.flowframework.workflow.WorkflowStepFactory.WorkflowSteps.getOutputByWorkflowType;
import static org.opensearch.flowframework.workflow.WorkflowStepFactory.WorkflowSteps.getRequiredPluginsByWorkflowType;
import static org.opensearch.flowframework.workflow.WorkflowStepFactory.WorkflowSteps.getTimeoutByWorkflowType;

/**
 * Converts a workflow of nodes and edges into a topologically sorted list of Process Nodes.
 */
public class WorkflowProcessSorter {

    private static final Logger logger = LogManager.getLogger(WorkflowProcessSorter.class);

    private WorkflowStepFactory workflowStepFactory;
    private ThreadPool threadPool;
    private Integer maxWorkflowSteps;
    private ClusterService clusterService;
    private Client client;

    /**
     * Instantiate this class.
     *
     * @param workflowStepFactory The factory which matches template step types to instances.
     * @param threadPool The OpenSearch Thread pool to pass to process nodes.
     * @param clusterService The OpenSearch cluster service.
     * @param client The OpenSearch Client
     * @param flowFrameworkSettings settings of the plugin
     */
    public WorkflowProcessSorter(
        WorkflowStepFactory workflowStepFactory,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        this.workflowStepFactory = workflowStepFactory;
        this.threadPool = threadPool;
        this.maxWorkflowSteps = flowFrameworkSettings.getMaxWorkflowSteps();
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * Sort a workflow into a topologically sorted list of process nodes.
     * @param workflow A workflow with (unsorted) nodes and edges which define predecessors and successors
     * @param workflowId The workflowId associated with the step
     * @param params Parameters passed on the REST path
     * @return A list of Process Nodes sorted topologically.  All predecessors of any node will occur prior to it in the list.
     */
    public List<ProcessNode> sortProcessNodes(Workflow workflow, String workflowId, Map<String, String> params) {
        if (workflow.nodes().size() > this.maxWorkflowSteps) {
            throw new FlowFrameworkException(
                "Workflow "
                    + workflowId
                    + " has "
                    + workflow.nodes().size()
                    + " nodes, which exceeds the maximum of "
                    + this.maxWorkflowSteps
                    + ". Change the setting ["
                    + MAX_WORKFLOW_STEPS.getKey()
                    + "] to increase this.",
                RestStatus.BAD_REQUEST
            );
        }
        List<WorkflowNode> sortedNodes = topologicalSort(workflow.nodes(), workflow.edges());

        List<ProcessNode> nodes = new ArrayList<>();
        Map<String, ProcessNode> idToNodeMap = new HashMap<>();
        for (WorkflowNode node : sortedNodes) {
            WorkflowStep step = workflowStepFactory.createStep(node.type());
            WorkflowData data = new WorkflowData(node.userInputs(), workflow.userParams(), workflowId, node.id());
            List<ProcessNode> predecessorNodes = workflow.edges()
                .stream()
                .filter(e -> e.destination().equals(node.id()))
                // since we are iterating in topological order we know all predecessors will be in the map
                .map(e -> idToNodeMap.get(e.source()))
                .collect(Collectors.toList());

            TimeValue nodeTimeout = parseTimeout(node);
            ProcessNode processNode = new ProcessNode(
                node.id(),
                step,
                node.previousNodeInputs(),
                params,
                data,
                predecessorNodes,
                threadPool,
                PROVISION_WORKFLOW_THREAD_POOL,
                nodeTimeout
            );
            idToNodeMap.put(processNode.id(), processNode);
            nodes.add(processNode);
        }
        return nodes;
    }

    /**
     * Sort an updated workflow into a topologically sorted list of create/update process nodes
     * @param workflowId the workflow ID associated with the template
     * @param originalTemplate the original template currently indexed
     * @param updatedTemplate the updated template to be executed
     * @param resourcesCreated the resources previously created for the workflow
     * @return
     */
    public List<ProcessNode> createReprovisionSequence(
        String workflowId,
        Template originalTemplate,
        Template updatedTemplate,
        List<ResourceCreated> resourcesCreated
    ) {

        Workflow updatedWorkflow = updatedTemplate.workflows().get(PROVISION_WORKFLOW);
        if (updatedWorkflow.nodes().size() > this.maxWorkflowSteps) {
            throw new FlowFrameworkException(
                "Workflow "
                    + workflowId
                    + " has "
                    + updatedWorkflow.nodes().size()
                    + " nodes, which exceeds the maximum of "
                    + this.maxWorkflowSteps
                    + ". Change the setting ["
                    + MAX_WORKFLOW_STEPS.getKey()
                    + "] to increase this.",
                RestStatus.BAD_REQUEST
            );
        }

        // Topologically sort the updated workflow
        List<WorkflowNode> sortedUpdatedNodes = topologicalSort(updatedWorkflow.nodes(), updatedWorkflow.edges());

        // Convert original template into node id map
        Map<String, WorkflowNode> originalTemplateMap = originalTemplate.workflows()
            .get(PROVISION_WORKFLOW)
            .nodes()
            .stream()
            .collect(Collectors.toMap(WorkflowNode::id, node -> node));

        // Temporarily block node deletions until fine-grained deprovisioning is implemented
        if (!originalTemplateMap.values().stream().allMatch(sortedUpdatedNodes::contains)) {
            throw new FlowFrameworkException(
                "Workflow Step deletion is not supported when reprovisioning a template.",
                RestStatus.BAD_REQUEST
            );
        }

        List<ProcessNode> reprovisionSequence = new ArrayList<>();
        Map<String, ProcessNode> idToNodeMap = new HashMap<>();

        // Iterate through sorted Updated Nodes
        for (WorkflowNode node : sortedUpdatedNodes) {

            WorkflowData data = new WorkflowData(node.userInputs(), updatedWorkflow.userParams(), workflowId, node.id());
            List<ProcessNode> predecessorNodes = updatedWorkflow.edges()
                .stream()
                .filter(e -> e.destination().equals(node.id()))
                // since we are iterating in topological order we know all predecessors will be in the map
                .map(e -> idToNodeMap.get(e.source()))
                .collect(Collectors.toList());

            TimeValue nodeTimeout = parseTimeout(node);

            if (!originalTemplateMap.containsKey(node.id())) {

                logger.info("TESTING : Node : " + node.id() + " is an additive modification!");

                // Case 1 : Additive modification, create new node

                WorkflowStep step = workflowStepFactory.createStep(node.type());

                ProcessNode processNode = new ProcessNode(
                    node.id(),
                    step,
                    node.previousNodeInputs(),
                    Collections.emptyMap(), // TODO Add support to reprovision substitution templates
                    data,
                    predecessorNodes,
                    threadPool,
                    PROVISION_WORKFLOW_THREAD_POOL,
                    nodeTimeout
                );
                idToNodeMap.put(processNode.id(), processNode);
                reprovisionSequence.add(processNode);

            } else {

                logger.info("TESTING : Node : " + node.id() + " is an existing modification!");

                // Case 2 : Existing Modification, compare previous node inputs and user inputs
                WorkflowNode originalNode = originalTemplateMap.get(node.id());

                Map<String, Object> updatedNodeUserInputs = node.userInputs();
                Map<String, Object> originalNodeUserInputs = originalNode.userInputs();

                boolean userInputsIsUpdated = false;
                for (String key : updatedNodeUserInputs.keySet()) {
                    Object updatedValue = updatedNodeUserInputs.get(key);
                    Object originalValue = originalNodeUserInputs.get(key);
                    if (!Objects.equals(updatedValue, originalValue)) {
                        userInputsIsUpdated = true;
                        break;
                    }
                }

                if (!node.previousNodeInputs().equals(originalNode.previousNodeInputs()) || userInputsIsUpdated) {

                    logger.info("TESTING : Node : " + node.id() + " needs to be updated");

                    // Create Update Step (if one is available)
                    String updateStepName = WorkflowResources.getUpdateStepByWorkflowStep(node.type());
                    if (updateStepName != null) {
                        WorkflowStep step = workflowStepFactory.createStep(updateStepName);
                        ProcessNode processNode = new ProcessNode(
                            node.id(),
                            step,
                            node.previousNodeInputs(),
                            Collections.emptyMap(), // TODO Add support to reprovision substitution templates
                            data,
                            predecessorNodes,
                            threadPool,
                            PROVISION_WORKFLOW_THREAD_POOL,
                            nodeTimeout
                        );
                        idToNodeMap.put(processNode.id(), processNode);
                        reprovisionSequence.add(processNode);
                    } else {

                        // Case 3 : Cannot update step (not supported)
                        logger.info("TESTING : Node : " + node.id() + "has changed inputs and does not support updates");
                        throw new FlowFrameworkException(
                            "Workflow Step " + node.id() + " does not support updates when reprovisioning.",
                            RestStatus.BAD_REQUEST
                        );

                    }
                } else {

                    // Case 4 : No modification to existing node, create proxy step to pass down required input to dependent nodes
                    logger.info("TESTING : Node : " + node.id() + "needs to get resources");

                    // Node ID should give us resources created
                    ResourceCreated nodeResource = null;
                    for (ResourceCreated resourceCreated : resourcesCreated) {
                        if (resourceCreated.workflowStepId().equals(node.id())) {
                            logger.info(
                                "TESTING : FOUND RESOURCE : Resource Created workflow Step ID : " + resourceCreated.workflowStepId()
                            );
                            nodeResource = resourceCreated;
                        }
                    }

                    if (nodeResource != null) {
                        // create process node
                        ProcessNode processNode = new ProcessNode(
                            node.id(),
                            new GetResourceStep(nodeResource),
                            node.previousNodeInputs(),
                            Collections.emptyMap(),
                            data,
                            predecessorNodes,
                            threadPool,
                            PROVISION_WORKFLOW_THREAD_POOL,
                            nodeTimeout
                        );
                        idToNodeMap.put(processNode.id(), processNode);
                        reprovisionSequence.add(processNode);

                    }

                }

            }

        }

        // If the reprovision sequence consists entirely of GetResourceSteps, then no modifications were made to the exisiting template.
        if (reprovisionSequence.stream().allMatch(n -> n.workflowStep().getName().equals(GetResourceStep.NAME))) {
            throw new FlowFrameworkException("Template does not contain any modifications", RestStatus.BAD_REQUEST);
        }

        return reprovisionSequence;
    }

    /**
     * Validates inputs and ensures the required plugins are installed for each step in a topologically sorted graph
     * @param processNodes the topologically sorted list of process nodes
     * @param pluginsService the Plugins Service to retrieve installed plugins
     * @throws Exception if validation fails
     */
    public void validate(List<ProcessNode> processNodes, PluginsService pluginsService) throws Exception {
        List<String> installedPlugins = pluginsService.info()
            .getPluginInfos()
            .stream()
            .map(PluginInfo::getName)
            .collect(Collectors.toList());
        validatePluginsInstalled(processNodes, installedPlugins);
        validateGraph(processNodes);
    }

    /**
     * Validates a sorted workflow, determines if each process node's required plugins are currently installed
     * @param processNodes A list of process nodes
     * @param installedPlugins The list of installed plugins
     * @throws Exception on validation failure
     */
    public void validatePluginsInstalled(List<ProcessNode> processNodes, List<String> installedPlugins) throws Exception {
        // Iterate through process nodes in graph
        for (ProcessNode processNode : processNodes) {

            // Retrieve required plugins of this node based on type
            String nodeType = processNode.workflowStep().getName();
            List<String> requiredPlugins = new ArrayList<>(getRequiredPluginsByWorkflowType(nodeType));
            if (!installedPlugins.containsAll(requiredPlugins)) {
                requiredPlugins.removeAll(installedPlugins);
                throw new FlowFrameworkException(
                    "The workflowStep "
                        + processNode.workflowStep().getName()
                        + " requires the following plugins to be installed : "
                        + requiredPlugins.toString(),
                    RestStatus.BAD_REQUEST
                );
            }
        }
    }

    /**
     * Validates a sorted workflow, determines if each process node's user inputs and predecessor outputs match the expected workflow step inputs
     * @param processNodes A list of process nodes
     * @throws Exception on validation failure
     */
    public void validateGraph(List<ProcessNode> processNodes) throws Exception {

        // Iterate through process nodes in graph
        for (ProcessNode processNode : processNodes) {

            // Get predecessor nodes types of this processNode
            List<String> predecessorNodeTypes = processNode.predecessors()
                .stream()
                .map(x -> x.workflowStep().getName())
                .collect(Collectors.toList());

            // Compile a list of outputs from the predecessor nodes based on type
            List<String> predecessorOutputs = predecessorNodeTypes.stream()
                .map(nodeType -> getOutputByWorkflowType(nodeType))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

            // Retrieve all the user input data from this node
            List<String> currentNodeUserInputs = new ArrayList<>(processNode.input().getContent().keySet());

            // Combine both predecessor outputs and current node user inputs
            List<String> allInputs = Stream.concat(predecessorOutputs.stream(), currentNodeUserInputs.stream())
                .collect(Collectors.toList());

            // Retrieve list of required inputs from the current process node and compare
            List<String> expectedInputs = new ArrayList<>(getInputByWorkflowType(processNode.workflowStep().getName()));

            if (!allInputs.containsAll(expectedInputs)) {
                expectedInputs.removeAll(allInputs);
                throw new FlowFrameworkException(
                    "Invalid workflow, node ["
                        + processNode.id()
                        + "] missing the following required inputs : "
                        + expectedInputs.toString(),
                    RestStatus.BAD_REQUEST
                );
            }
        }
    }

    /**
     * A method for parsing workflow timeout value.
     * The value could be parsed from node NODE_TIMEOUT_FIELD, the timeout field in workflow-step.json,
     * or the default NODE_TIMEOUT_DEFAULT_VALUE
     * @param node the workflow node
     * @return the timeout value
     */
    protected TimeValue parseTimeout(WorkflowNode node) {
        TimeValue nodeTimeoutValue = Optional.ofNullable(getTimeoutByWorkflowType(node.type())).orElse(NODE_TIMEOUT_DEFAULT_VALUE);
        String nodeTimeoutAsString = nodeTimeoutValue.getSeconds() + "s";
        String timeoutValue = (String) node.userInputs().getOrDefault(NODE_TIMEOUT_FIELD, nodeTimeoutAsString);
        String fieldName = String.join(".", node.id(), USER_INPUTS_FIELD, NODE_TIMEOUT_FIELD);
        TimeValue userInputTimeValue = TimeValue.parseTimeValue(timeoutValue, fieldName);

        if (userInputTimeValue.millis() < 0) {
            throw new FlowFrameworkException(
                "Failed to parse timeout value [" + timeoutValue + "] for field [" + fieldName + "]. Must be positive",
                RestStatus.BAD_REQUEST
            );
        }
        return userInputTimeValue;
    }

    private static List<WorkflowNode> topologicalSort(List<WorkflowNode> workflowNodes, List<WorkflowEdge> workflowEdges) {
        // Basic validation
        Map<String, WorkflowNode> nodeMap = new HashMap<>();
        for (WorkflowNode node : workflowNodes) {
            if (nodeMap.containsKey(node.id())) {
                throw new FlowFrameworkException("Duplicate node id " + node.id() + ".", RestStatus.BAD_REQUEST);
            }
            nodeMap.put(node.id(), node);
        }
        for (WorkflowEdge edge : workflowEdges) {
            String source = edge.source();
            if (!nodeMap.containsKey(source)) {
                throw new FlowFrameworkException("Edge source " + source + " does not correspond to a node.", RestStatus.BAD_REQUEST);
            }
            String dest = edge.destination();
            if (!nodeMap.containsKey(dest)) {
                throw new FlowFrameworkException("Edge destination " + dest + " does not correspond to a node.", RestStatus.BAD_REQUEST);
            }
            if (source.equals(dest)) {
                throw new FlowFrameworkException("Edge connects node " + source + " to itself.", RestStatus.BAD_REQUEST);
            }
        }

        // Build predecessor and successor maps
        Map<WorkflowNode, Set<WorkflowEdge>> predecessorEdges = new HashMap<>();
        Map<WorkflowNode, Set<WorkflowEdge>> successorEdges = new HashMap<>();
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
        Queue<WorkflowNode> sourceNodes = workflowNodes.stream()
            .filter(n -> !predecessorEdges.containsKey(n))
            .collect(ArrayDeque::new, ArrayDeque::add, ArrayDeque::addAll);
        if (sourceNodes.isEmpty()) {
            throw new FlowFrameworkException("No start node detected: all nodes have a predecessor.", RestStatus.BAD_REQUEST);
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
                if (predecessorEdges.get(m).stream().noneMatch(graph::contains)) {
                    // insert m into S
                    sourceNodes.add(m);
                }
            }
        }
        if (!graph.isEmpty()) {
            throw new FlowFrameworkException("Cycle detected: " + graph, RestStatus.BAD_REQUEST);
        }
        logger.debug("Execution sequence: {}", sortedNodes);
        return sortedNodes;
    }
}
