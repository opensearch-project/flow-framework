/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.WorkflowResources.getDeprovisionStepByWorkflowStep;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * Transport Action to deprovision a workflow from a stored use case template
 */
public class DeprovisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private static final String DEPROVISION_SUFFIX = "_deprovision";

    private final Logger logger = LogManager.getLogger(DeprovisionWorkflowTransportAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final WorkflowProcessSorter workflowProcessSorter;
    private final WorkflowStepFactory workflowStepFactory;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final EncryptorUtils encryptorUtils;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param threadPool The OpenSearch thread pool
     * @param client The node client to retrieve a stored use case template
     * @param workflowProcessSorter Utility class to generate a togologically sorted list of Process nodes
     * @param workflowStepFactory The factory instantiating workflow steps
     * @param flowFrameworkIndicesHandler Class to handle all internal system indices actions
     * @param encryptorUtils Utility class to handle encryption/decryption
     */
    @Inject
    public DeprovisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        WorkflowProcessSorter workflowProcessSorter,
        WorkflowStepFactory workflowStepFactory,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        EncryptorUtils encryptorUtils
    ) {
        super(DeprovisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.workflowProcessSorter = workflowProcessSorter;
        this.workflowStepFactory = workflowStepFactory;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.encryptorUtils = encryptorUtils;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {
        // Retrieve use case template from global context
        String workflowId = request.getWorkflowId();
        GetRequest getRequest = new GetRequest(GLOBAL_CONTEXT_INDEX, workflowId);

        // Stash thread context to interact with system index
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            client.get(getRequest, ActionListener.wrap(response -> {
                context.restore();

                if (!response.isExists()) {
                    listener.onFailure(
                        new FlowFrameworkException(
                            "Failed to retrieve template (" + workflowId + ") from global context.",
                            RestStatus.NOT_FOUND
                        )
                    );
                    return;
                }

                // Parse template from document source
                Template template = Template.parse(response.getSourceAsString());

                // Decrypt template
                template = encryptorUtils.decryptTemplateCredentials(template);

                // Sort and validate graph
                Workflow provisionWorkflow = template.workflows().get(PROVISION_WORKFLOW);
                List<ProcessNode> provisionProcessSequence = workflowProcessSorter.sortProcessNodes(provisionWorkflow, workflowId);
                workflowProcessSorter.validateGraph(provisionProcessSequence);

                // We have a valid template and sorted nodes, get the created resources
                getResourcesAndExecute(request.getWorkflowId(), provisionProcessSequence, listener);
            }, exception -> {
                if (exception instanceof FlowFrameworkException) {
                    logger.error("Workflow validation failed for workflow : " + workflowId);
                    listener.onFailure(exception);
                } else {
                    logger.error("Failed to retrieve template from global context.", exception);
                    listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
                }
            }));
        } catch (Exception e) {
            logger.error("Failed to retrieve template from global context.", e);
            listener.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
        }
    }

    private void getResourcesAndExecute(
        String workflowId,
        List<ProcessNode> provisionProcessSequence,
        ActionListener<WorkflowResponse> listener
    ) {
        GetWorkflowRequest getRequest = new GetWorkflowRequest(workflowId, true);
        client.execute(GetWorkflowAction.INSTANCE, getRequest, ActionListener.wrap(response -> {
            // Get a map of step id to created resources
            final Map<String, ResourceCreated> resourceMap = response.getWorkflowState()
                .resourcesCreated()
                .stream()
                .collect(Collectors.toMap(ResourceCreated::workflowStepId, Function.identity()));

            // Now finally do the deprovision
            executeDeprovisionSequence(workflowId, resourceMap, provisionProcessSequence, listener);
        }, exception -> {
            logger.error("Failed to get workflow state for workflow " + workflowId);
            listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
        }));
    }

    private void executeDeprovisionSequence(
        String workflowId,
        Map<String, ResourceCreated> resourceMap,
        List<ProcessNode> provisionProcessSequence,
        ActionListener<WorkflowResponse> listener
    ) {
        // Create a list of ProcessNodes with ta corresponding deprovision workflow steps
        List<ProcessNode> deprovisionProcessSequence = provisionProcessSequence.stream()
            // Only include nodes that created a resource
            .filter(pn -> resourceMap.containsKey(pn.id()))
            // Create a new ProcessNode with a deprovision step
            .map(pn -> {
                String stepName = pn.workflowStep().getName();
                String deprovisionStep = getDeprovisionStepByWorkflowStep(stepName);
                // Unimplemented steps presently return null, so skip
                if (deprovisionStep == null) {
                    return null;
                }
                // New ID is old ID with deprovision added
                String deprovisionStepId = pn.id() + DEPROVISION_SUFFIX;
                return new ProcessNode(
                    deprovisionStepId,
                    workflowStepFactory.createStep(deprovisionStep),
                    Collections.emptyMap(),
                    new WorkflowData(
                        Map.of(getResourceByWorkflowStep(stepName), resourceMap.get(pn.id()).resourceId()),
                        workflowId,
                        deprovisionStepId
                    ),
                    Collections.emptyList(),
                    this.threadPool,
                    pn.nodeTimeout()
                );
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        // Deprovision in reverse order of provisioning to minimize risk of dependencies
        Collections.reverse(deprovisionProcessSequence);
        logger.info("Deprovisioning steps: {}", deprovisionProcessSequence.stream().map(ProcessNode::id).collect(Collectors.joining(", ")));

        // Repeat attempting to delete resources as long as at least one is successful
        int resourceCount = deprovisionProcessSequence.size();
        while (resourceCount > 0) {
            Iterator<ProcessNode> iter = deprovisionProcessSequence.iterator();
            while (iter.hasNext()) {
                ProcessNode deprovisionNode = iter.next();
                String resourceNameAndId = getResourceFromProcessNode(deprovisionNode, resourceMap);
                CompletableFuture<WorkflowData> deprovisionFuture = deprovisionNode.execute();
                try {
                    deprovisionFuture.join();
                    logger.info("Successful {} for {}", deprovisionNode.id(), resourceNameAndId);
                    // Remove from list so we don't try again
                    iter.remove();
                } catch (Throwable t) {
                    logger.info(
                        "Failed {} for {}: {}",
                        deprovisionNode.id(),
                        resourceNameAndId,
                        t.getCause() == null ? t.getMessage() : t.getCause().getMessage()
                    );
                }
                if (deprovisionFuture.isCompletedExceptionally()) {} else {}
            }
            if (deprovisionProcessSequence.size() < resourceCount) {
                // If we've deleted something, decrement and try again if not zero
                resourceCount = deprovisionProcessSequence.size();
                deprovisionProcessSequence = deprovisionProcessSequence.stream().map(pn -> {
                    return new ProcessNode(
                        pn.id(),
                        workflowStepFactory.createStep(pn.workflowStep().getName()),
                        pn.previousNodeInputs(),
                        pn.input(),
                        pn.predecessors(),
                        this.threadPool,
                        pn.nodeTimeout()
                    );
                }).collect(Collectors.toList());
            } else {
                // If nothing was deleted, exit loop
                break;
            }
        }
        if (deprovisionProcessSequence.isEmpty()) {
            // Successful deprovision, return workflow ID
            // TODO: Reset state for this workflow ID to NOT STARTED
            listener.onResponse(new WorkflowResponse(workflowId));
        } else {
            // Failed deprovision, give user list of remaining resources
            listener.onFailure(
                // TODO: Update state for this workflow ID to just the remaining resources
                new FlowFrameworkException(
                    "Failed to deprovision some resources: ["
                        + deprovisionProcessSequence.stream()
                            .map(pn -> getResourceFromProcessNode(pn, resourceMap))
                            .collect(Collectors.joining(", "))
                        + "].",
                    RestStatus.ACCEPTED
                )
            );
        }
    }

    private String getResourceFromProcessNode(ProcessNode deprovisionNode, Map<String, ResourceCreated> resourceMap) {
        String deprovisionId = deprovisionNode.id();
        int pos = deprovisionId.indexOf(DEPROVISION_SUFFIX);
        if (pos > 0) {
            String stepName = deprovisionId.substring(0, pos);
            return getResourceByWorkflowStep(deprovisionNode.workflowStep().getName()) + " " + resourceMap.get(stepName).resourceId();
        }
        return null;
    }
}
