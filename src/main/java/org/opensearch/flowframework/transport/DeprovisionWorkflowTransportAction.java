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
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.util.TenantAwareHelper;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.ALLOW_DELETE;
import static org.opensearch.flowframework.common.CommonValue.DEPROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_END_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.RESOURCES_CREATED_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.common.WorkflowResources.getDeprovisionStepByWorkflowStep;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;
import static org.opensearch.flowframework.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.flowframework.util.ParseUtils.verifyResourceAccessAndProcessRequest;

/**
 * Transport Action to deprovision a workflow from a stored use case template
 */
public class DeprovisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(DeprovisionWorkflowTransportAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final SdkClient sdkClient;
    private final WorkflowStepFactory workflowStepFactory;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param threadPool The OpenSearch thread pool
     * @param client The node client to retrieve a stored use case template
     * @param sdkClient the Multitenant Client
     * @param workflowStepFactory The factory instantiating workflow steps
     * @param flowFrameworkIndicesHandler Class to handle all internal system indices actions
     * @param flowFrameworkSettings The plugin settings
     * @param clusterService the cluster service
     * @param xContentRegistry contentRegister to parse get response
     * @param settings the plugin settings
     */
    @Inject
    public DeprovisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        SdkClient sdkClient,
        WorkflowStepFactory workflowStepFactory,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings
    ) {
        super(DeprovisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.sdkClient = sdkClient;
        this.workflowStepFactory = workflowStepFactory;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> workflowListener) {
        final String tenantId = request.getTemplate() == null ? null : request.getTemplate().getTenantId();
        if (!TenantAwareHelper.validateTenantId(flowFrameworkSettings.isMultiTenancyEnabled(), tenantId, workflowListener)) {
            return;
        }
        if (!TenantAwareHelper.tryAcquireDeprovision(
            flowFrameworkSettings.getMaxActiveDeprovisionsPerTenant(),
            tenantId,
            workflowListener
        )) {
            return;
        }
        ActionListener<WorkflowResponse> listener = TenantAwareHelper.releaseDeprovisionListener(tenantId, workflowListener);
        String workflowId = request.getWorkflowId();
        User user = getUserContext(client);

        // Stash thread context to interact with system index
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            verifyResourceAccessAndProcessRequest(
                CommonValue.WORKFLOW_RESOURCE_TYPE,
                () -> executeDeprovisionRequest(request, tenantId, listener, context, user),
                () -> resolveUserAndExecute(
                    user,
                    workflowId,
                    tenantId,
                    filterByEnabled,
                    true,
                    flowFrameworkSettings.isMultiTenancyEnabled(),
                    listener,
                    () -> executeDeprovisionRequest(request, tenantId, listener, context, user),
                    client,
                    sdkClient,
                    clusterService,
                    xContentRegistry
                )
            );
        } catch (Exception e) {
            String errorMessage = "Failed to retrieve template from global context.";
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

    private void executeDeprovisionRequest(
        WorkflowRequest request,
        String tenantId,
        ActionListener<WorkflowResponse> listener,
        ThreadContext.StoredContext context,
        User user
    ) {
        String workflowId = request.getWorkflowId();
        String allowDelete = request.getParams().get(ALLOW_DELETE);
        GetWorkflowStateRequest getStateRequest = new GetWorkflowStateRequest(workflowId, true, tenantId);
        logger.info("Querying state for workflow: {}", workflowId);
        client.execute(GetWorkflowStateAction.INSTANCE, getStateRequest, ActionListener.wrap(response -> {
            context.restore();

            Set<String> deleteAllowedResources = Strings.tokenizeByCommaToSet(allowDelete);
            // Retrieve resources from workflow state and deprovision
            threadPool.executor(DEPROVISION_WORKFLOW_THREAD_POOL)
                .execute(
                    () -> executeDeprovisionSequence(
                        workflowId,
                        tenantId,
                        response.getWorkflowState().resourcesCreated(),
                        deleteAllowedResources,
                        listener,
                        user
                    )
                );
        }, exception -> {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to get workflow state for workflow {}",
                workflowId
            ).getFormattedMessage();
            logger.error(errorMessage, exception);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
        }));
    }

    private void executeDeprovisionSequence(
        String workflowId,
        String tenantId,
        List<ResourceCreated> resourcesCreated,
        Set<String> deleteAllowedResources,
        ActionListener<WorkflowResponse> listener,
        User user
    ) {
        List<ResourceCreated> deleteNotAllowed = new ArrayList<>();
        // Create a list of ProcessNodes with the corresponding deprovision workflow steps
        List<ProcessNode> deprovisionProcessSequence = new ArrayList<>();
        for (ResourceCreated resource : resourcesCreated) {
            String workflowStepId = resource.workflowStepId();

            String stepName = resource.workflowStepName();
            WorkflowStep deprovisionStep = workflowStepFactory.createStep(getDeprovisionStepByWorkflowStep(stepName));
            // Skip if the step requires allow_delete but the resourceId isn't included
            if (deprovisionStep.allowDeleteRequired() && !deleteAllowedResources.contains(resource.resourceId())) {
                deleteNotAllowed.add(resource);
                continue;
            }
            // New ID is old ID with (deprovision step type) prepended
            String deprovisionStepId = "(deprovision_" + stepName + ") " + workflowStepId;
            deprovisionProcessSequence.add(
                new ProcessNode(
                    deprovisionStepId,
                    deprovisionStep,
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    new WorkflowData(Map.of(getResourceByWorkflowStep(stepName), resource.resourceId()), workflowId, deprovisionStepId),
                    Collections.emptyList(),
                    this.threadPool,
                    DEPROVISION_WORKFLOW_THREAD_POOL,
                    flowFrameworkSettings.getRequestTimeout(),
                    tenantId
                )
            );
        }

        // Deprovision in reverse order of provisioning to minimize risk of dependencies
        Collections.reverse(deprovisionProcessSequence);
        logger.info("Deprovisioning steps: {}", deprovisionProcessSequence.stream().map(ProcessNode::id).collect(Collectors.joining(", ")));

        // Repeat attempting to delete resources as long as at least one is successful
        int resourceCount = deprovisionProcessSequence.size();
        while (resourceCount > 0) {
            PlainActionFuture<WorkflowData> stateUpdateFuture;
            Iterator<ProcessNode> iter = deprovisionProcessSequence.iterator();
            do {
                ProcessNode deprovisionNode = iter.next();
                ResourceCreated resource = getResourceFromDeprovisionNode(deprovisionNode, resourcesCreated);
                String resourceNameAndId = getResourceNameAndId(resource);
                PlainActionFuture<WorkflowData> deprovisionFuture = deprovisionNode.execute();
                stateUpdateFuture = PlainActionFuture.newFuture();
                try {
                    deprovisionFuture.get();
                    logger.info("Successful {} for {}", deprovisionNode.id(), resourceNameAndId);
                    // Remove from state index resource list
                    flowFrameworkIndicesHandler.deleteResourceFromStateIndex(workflowId, tenantId, resource, stateUpdateFuture);
                    try {
                        // Wait at most 1 second for state index update.
                        stateUpdateFuture.actionGet(1, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        // Ignore incremental resource removal failures (or timeouts) as we catch up at the end with remainingResources
                    }
                    // Remove from list so we don't try again
                    iter.remove();
                    // Pause briefly before next step
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Throwable t) {
                    // If any deprovision fails due to not found, it's a success
                    if (t.getCause() instanceof OpenSearchStatusException
                        && ((OpenSearchStatusException) t.getCause()).status() == RestStatus.NOT_FOUND) {
                        logger.info("Successful (not found) {} for {}", deprovisionNode.id(), resourceNameAndId);
                        // Remove from list so we don't try again
                        iter.remove();
                    } else {
                        logger.info("Failed {} for {}", deprovisionNode.id(), resourceNameAndId);
                    }
                }
            } while (iter.hasNext());
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
                        pn.nodeTimeout(),
                        tenantId
                    );
                }).collect(Collectors.toList());
                // Pause briefly before next loop
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
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
        logger.info("Resources remaining: {}.", remainingResources);
        if (!deleteNotAllowed.isEmpty()) {
            logger.info("Resources requiring allow_delete: {}.", deleteNotAllowed);
        }
        // This is a redundant best-effort backup to the incremental deletion done earlier
        updateWorkflowState(workflowId, tenantId, remainingResources, deleteNotAllowed, listener, user);
    }

    private void updateWorkflowState(
        String workflowId,
        String tenantId,
        List<ResourceCreated> remainingResources,
        List<ResourceCreated> deleteNotAllowed,
        ActionListener<WorkflowResponse> listener,
        User user
    ) {
        if (remainingResources.isEmpty() && deleteNotAllowed.isEmpty()) {
            // Successful deprovision of all resources, reset state to initial
            flowFrameworkIndicesHandler.doesTemplateExist(workflowId, tenantId, templateExists -> {
                if (Boolean.TRUE.equals(templateExists)) {
                    flowFrameworkIndicesHandler.putInitialStateToWorkflowState(
                        workflowId,
                        tenantId,
                        user,
                        ActionListener.wrap(indexResponse -> {
                            logger.info("Reset workflow {} state to NOT_STARTED", workflowId);
                        }, exception -> { logger.error("Failed to reset to initial workflow state for {}", workflowId, exception); })
                    );
                } else {
                    flowFrameworkIndicesHandler.deleteFlowFrameworkSystemIndexDoc(
                        workflowId,
                        tenantId,
                        ActionListener.wrap(deleteResponse -> {
                            logger.info("Deleted workflow {} state", workflowId);
                        }, exception -> { logger.error("Failed to delete workflow state for {}", workflowId, exception); })
                    );
                }
                // return workflow ID
                listener.onResponse(new WorkflowResponse(workflowId));
            }, listener);
        } else {
            // Remaining resources only includes ones we tried to delete
            List<ResourceCreated> stateIndexResources = new ArrayList<>(remainingResources);
            // Add in those we skipped
            stateIndexResources.addAll(deleteNotAllowed);
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                tenantId,
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.COMPLETED),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.DONE),
                    Map.entry(PROVISION_END_TIME_FIELD, Instant.now().toEpochMilli()),
                    Map.entry(RESOURCES_CREATED_FIELD, stateIndexResources)
                ),
                ActionListener.wrap(updateResponse -> {
                    logger.info("updated workflow {} state to COMPLETED", workflowId);
                }, exception -> { logger.error("Failed to update workflow {} state", workflowId, exception); })
            );
            // give user list of remaining resources
            StringBuilder message = new StringBuilder();
            appendResourceInfo(message, "Failed to deprovision some resources: ", remainingResources);
            appendResourceInfo(message, "These resources require the " + ALLOW_DELETE + " parameter to deprovision: ", deleteNotAllowed);
            listener.onFailure(
                new FlowFrameworkException(message.toString(), remainingResources.isEmpty() ? RestStatus.FORBIDDEN : RestStatus.ACCEPTED)
            );
        }
    }

    private static void appendResourceInfo(StringBuilder message, String prefix, List<ResourceCreated> resources) {
        if (!resources.isEmpty()) {
            if (message.length() > 0) {
                message.append(" ");
            }
            message.append(prefix)
                .append(
                    resources.stream()
                        .map(DeprovisionWorkflowTransportAction::getResourceNameAndId)
                        .filter(Objects::nonNull)
                        .distinct()
                        .collect(Collectors.joining(", ", "[", "]"))
                )
                .append(".");
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
