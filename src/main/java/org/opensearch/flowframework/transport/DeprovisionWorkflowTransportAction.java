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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.DEPROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_START_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.RESOURCES_CREATED_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.common.WorkflowResources.getDeprovisionStepByWorkflowStep;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * Transport Action to deprovision a workflow from a stored use case template
 */
public class DeprovisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(DeprovisionWorkflowTransportAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final WorkflowStepFactory workflowStepFactory;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final FlowFrameworkSettings flowFrameworkSettings;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param threadPool The OpenSearch thread pool
     * @param client The node client to retrieve a stored use case template
     * @param workflowStepFactory The factory instantiating workflow steps
     * @param flowFrameworkIndicesHandler Class to handle all internal system indices actions
     * @param flowFrameworkSettings The plugin settings
     */
    @Inject
    public DeprovisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        WorkflowStepFactory workflowStepFactory,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        super(DeprovisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.workflowStepFactory = workflowStepFactory;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {
        String workflowId = request.getWorkflowId();
        GetWorkflowStateRequest getStateRequest = new GetWorkflowStateRequest(workflowId, true);

        // Stash thread context to interact with system index
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            client.execute(GetWorkflowStateAction.INSTANCE, getStateRequest, ActionListener.wrap(response -> {
                context.restore();

                // Retrieve resources from workflow state and deprovision
                threadPool.executor(DEPROVISION_WORKFLOW_THREAD_POOL)
                    .execute(() -> executeDeprovisionSequence(workflowId, response.getWorkflowState().resourcesCreated(), listener));
            }, exception -> {
                String message = "Failed to get workflow state for workflow " + workflowId;
                logger.error(message, exception);
                listener.onFailure(new FlowFrameworkException(message, ExceptionsHelper.status(exception)));
            }));
        } catch (Exception e) {
            String message = "Failed to retrieve template from global context.";
            logger.error(message, e);
            listener.onFailure(new FlowFrameworkException(message, ExceptionsHelper.status(e)));
        }
    }

    private void executeDeprovisionSequence(
        String workflowId,
        List<ResourceCreated> resourcesCreated,
        ActionListener<WorkflowResponse> listener
    ) {

        // Create a list of ProcessNodes with the corresponding deprovision workflow steps
        List<ProcessNode> deprovisionProcessSequence = new ArrayList<>();
        for (ResourceCreated resource : resourcesCreated) {
            String workflowStepId = resource.workflowStepId();

            String stepName = resource.workflowStepName();
            String deprovisionStep = getDeprovisionStepByWorkflowStep(stepName);
            // Unimplemented steps presently return null, so skip
            if (deprovisionStep == null) {
                continue;
            }
            // New ID is old ID with (deprovision step type) prepended
            String deprovisionStepId = "(deprovision_" + stepName + ") " + workflowStepId;
            deprovisionProcessSequence.add(
                new ProcessNode(
                    deprovisionStepId,
                    workflowStepFactory.createStep(deprovisionStep),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    new WorkflowData(Map.of(getResourceByWorkflowStep(stepName), resource.resourceId()), workflowId, deprovisionStepId),
                    Collections.emptyList(),
                    this.threadPool,
                    DEPROVISION_WORKFLOW_THREAD_POOL,
                    flowFrameworkSettings.getRequestTimeout()
                )
            );
        }

        // Deprovision in reverse order of provisioning to minimize risk of dependencies
        Collections.reverse(deprovisionProcessSequence);
        logger.info("Deprovisioning steps: {}", deprovisionProcessSequence.stream().map(ProcessNode::id).collect(Collectors.joining(", ")));

        // Repeat attempting to delete resources as long as at least one is successful
        int resourceCount = deprovisionProcessSequence.size();
        while (resourceCount > 0) {
            Iterator<ProcessNode> iter = deprovisionProcessSequence.iterator();
            while (iter.hasNext()) {
                ProcessNode deprovisionNode = iter.next();
                ResourceCreated resource = getResourceFromDeprovisionNode(deprovisionNode, resourcesCreated);
                String resourceNameAndId = getResourceNameAndId(resource);
                PlainActionFuture<WorkflowData> deprovisionFuture = deprovisionNode.execute();
                try {
                    deprovisionFuture.get();
                    logger.info("Successful {} for {}", deprovisionNode.id(), resourceNameAndId);
                    // Remove from list so we don't try again
                    iter.remove();
                    // Pause briefly before next step
                    Thread.sleep(100);
                } catch (Throwable t) {
                    // If any deprovision fails due to not found, it's a success
                    if (t.getCause() instanceof OpenSearchStatusException
                        && ((OpenSearchStatusException) t.getCause()).status() == RestStatus.NOT_FOUND) {
                        logger.info("Successful (not found) {} for {}", deprovisionNode.id(), resourceNameAndId);
                        // Remove from list so we don't try again
                        iter.remove();
                    } else {
                        logger.info(
                            "Failed {} for {}: {}",
                            deprovisionNode.id(),
                            resourceNameAndId,
                            t.getCause() == null ? t.getMessage() : t.getCause().getMessage()
                        );
                    }
                }
            }
            if (deprovisionProcessSequence.size() < resourceCount) {
                // If we've deleted something, decrement and try again if not zero
                resourceCount = deprovisionProcessSequence.size();
                deprovisionProcessSequence = deprovisionProcessSequence.stream().map(pn -> {
                    return new ProcessNode(
                        pn.id(),
                        workflowStepFactory.createStep(pn.workflowStep().getName()),
                        pn.previousNodeInputs(),
                        pn.params(),
                        pn.input(),
                        pn.predecessors(),
                        this.threadPool,
                        DEPROVISION_WORKFLOW_THREAD_POOL,
                        pn.nodeTimeout()
                    );
                }).collect(Collectors.toList());
                // Pause briefly before next loop
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            } else {
                // If nothing was deleted, exit loop
                break;
            }
        }
        // Get corresponding resources
        List<ResourceCreated> remainingResources = deprovisionProcessSequence.stream()
            .map(pn -> getResourceFromDeprovisionNode(pn, resourcesCreated))
            .collect(Collectors.toList());
        logger.info("Resources remaining: {}", remainingResources);
        updateWorkflowState(workflowId, remainingResources, listener);
    }

    private void updateWorkflowState(
        String workflowId,
        List<ResourceCreated> remainingResources,
        ActionListener<WorkflowResponse> listener
    ) {
        if (remainingResources.isEmpty()) {
            // Successful deprovision
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.NOT_STARTED),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.NOT_STARTED),
                    Map.entry(PROVISION_START_TIME_FIELD, Instant.now().toEpochMilli()),
                    Map.entry(RESOURCES_CREATED_FIELD, Collections.emptyList())
                ),
                ActionListener.wrap(updateResponse -> {
                    logger.info("updated workflow {} state to NOT_STARTED", workflowId);
                }, exception -> { logger.error("Failed to update workflow state : {}", exception.getMessage()); })
            );
            // return workflow ID
            listener.onResponse(new WorkflowResponse(workflowId));
        } else {
            // Failed deprovision
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.COMPLETED),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.DONE),
                    Map.entry(PROVISION_START_TIME_FIELD, Instant.now().toEpochMilli()),
                    Map.entry(RESOURCES_CREATED_FIELD, remainingResources)
                ),
                ActionListener.wrap(updateResponse -> {
                    logger.info("updated workflow {} state to COMPLETED", workflowId);
                }, exception -> { logger.error("Failed to update workflow state : {}", exception.getMessage()); })
            );
            // give user list of remaining resources
            listener.onFailure(
                new FlowFrameworkException(
                    "Failed to deprovision some resources: ["
                        + remainingResources.stream()
                            .map(DeprovisionWorkflowTransportAction::getResourceNameAndId)
                            .filter(Objects::nonNull)
                            .distinct()
                            .collect(Collectors.joining(", "))
                        + "].",
                    RestStatus.ACCEPTED
                )
            );
        }
    }

    private static ResourceCreated getResourceFromDeprovisionNode(ProcessNode deprovisionNode, List<ResourceCreated> resourcesCreated) {
        return resourcesCreated.stream()
            .filter(r -> deprovisionNode.id().equals("(deprovision_" + r.workflowStepName() + ") " + r.workflowStepId()))
            .findFirst()
            .orElse(null);
    }

    private static String getResourceNameAndId(ResourceCreated resource) {
        if (resource == null) {
            return null;
        }
        return getResourceByWorkflowStep(resource.workflowStepName()) + " " + resource.resourceId();
    }
}
