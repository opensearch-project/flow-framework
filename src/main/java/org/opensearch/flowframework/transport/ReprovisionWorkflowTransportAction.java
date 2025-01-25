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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.util.TenantAwareHelper;
import org.opensearch.flowframework.util.WorkflowTimeoutUtility;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.plugins.PluginsService;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.ERROR_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_END_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_START_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.RESOURCES_CREATED_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;
import static org.opensearch.flowframework.util.ParseUtils.resolveUserAndExecute;

/**
 * Transport Action to reprovision a provisioned template
 */
public class ReprovisionWorkflowTransportAction extends HandledTransportAction<ReprovisionWorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(ReprovisionWorkflowTransportAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final SdkClient sdkClient;
    private final WorkflowStepFactory workflowStepFactory;
    private final WorkflowProcessSorter workflowProcessSorter;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private final PluginsService pluginsService;
    private final EncryptorUtils encryptorUtils;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new ReprovisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param threadPool The OpenSearch thread pool
     * @param client The node client to retrieve a stored use case template
     * @param sdkClient the Multitenant Client
     * @param workflowStepFactory The factory instantiating workflow steps
     * @param workflowProcessSorter Utility class to generate a togologically sorted list of Process nodes
     * @param flowFrameworkIndicesHandler Class to handle all internal system indices actions
     * @param flowFrameworkSettings Whether this API is enabled
     * @param encryptorUtils Utility class to handle encryption/decryption
     * @param pluginsService The Plugins Service
     * @param clusterService The Cluster Service
     * @param xContentRegistry The XContent Registry
     * @param settings  The plugin settings
     */
    @Inject
    public ReprovisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        SdkClient sdkClient,
        WorkflowStepFactory workflowStepFactory,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        EncryptorUtils encryptorUtils,
        PluginsService pluginsService,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings
    ) {
        super(ReprovisionWorkflowAction.NAME, transportService, actionFilters, ReprovisionWorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.sdkClient = sdkClient;
        this.workflowStepFactory = workflowStepFactory;
        this.workflowProcessSorter = workflowProcessSorter;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
        this.encryptorUtils = encryptorUtils;
        this.pluginsService = pluginsService;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, ReprovisionWorkflowRequest request, ActionListener<WorkflowResponse> listener) {
        String tenantId = request.getUpdatedTemplate() == null ? null : request.getUpdatedTemplate().getTenantId();
        if (!TenantAwareHelper.validateTenantId(flowFrameworkSettings.isMultiTenancyEnabled(), tenantId, listener)) {
            return;
        }
        String workflowId = request.getWorkflowId();
        User user = getUserContext(client);

        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                workflowId,
                tenantId,
                filterByEnabled,
                false,
                flowFrameworkSettings.isMultiTenancyEnabled(),
                listener,
                () -> executeReprovisionRequest(request, tenantId, listener, context),
                client,
                sdkClient,
                clusterService,
                xContentRegistry
            );
        } catch (Exception e) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to get workflow state for workflow {}",
                workflowId
            ).getFormattedMessage();
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }

    }

    /**
     * Execute the reprovision request
     * @param request the reprovision request
     * @param tenantId
     * @param listener the action listener
     * @param context the thread context
     */
    private void executeReprovisionRequest(
        ReprovisionWorkflowRequest request,
        String tenantId,
        ActionListener<WorkflowResponse> listener,
        ThreadContext.StoredContext context
    ) {
        String workflowId = request.getWorkflowId();
        logger.info("Querying state for workflow: {}", workflowId);
        // Retrieve state and resources created
        GetWorkflowStateRequest getStateRequest = new GetWorkflowStateRequest(workflowId, true, tenantId);
        client.execute(GetWorkflowStateAction.INSTANCE, getStateRequest, ActionListener.wrap(response -> {
            context.restore();

            State currentState = State.valueOf(response.getWorkflowState().getState());
            if (State.PROVISIONING.equals(currentState) || State.NOT_STARTED.equals(currentState)) {
                String errorMessage = "The template can not be reprovisioned unless its provisioning state is DONE or FAILED: "
                    + workflowId;
                throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
            }

            // Generate reprovision sequence
            List<ResourceCreated> resourceCreated = response.getWorkflowState().resourcesCreated();

            // Original template is retrieved from index, attempt to decrypt any exisiting credentials before processing
            Template originalTemplate = encryptorUtils.decryptTemplateCredentials(request.getOriginalTemplate());
            Template updatedTemplate = request.getUpdatedTemplate();

            // Validate updated template prior to execution
            Workflow provisionWorkflow = updatedTemplate.workflows().get(PROVISION_WORKFLOW);
            List<ProcessNode> updatedProcessSequence = workflowProcessSorter.sortProcessNodes(
                provisionWorkflow,
                request.getWorkflowId(),
                Collections.emptyMap(), // TODO : Add suport to reprovision substitution templates
                tenantId
            );

            try {
                workflowProcessSorter.validate(updatedProcessSequence, pluginsService);
            } catch (Exception e) {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Workflow validation failed for workflow {}",
                    request.getWorkflowId()
                ).getFormattedMessage();
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
            }
            List<ProcessNode> reprovisionProcessSequence = workflowProcessSorter.createReprovisionSequence(
                workflowId,
                originalTemplate,
                updatedTemplate,
                resourceCreated,
                tenantId
            );

            // Remove error field if any prior to subsequent execution
            if (response.getWorkflowState().getError() != null) {
                WorkflowState newState = WorkflowState.builder(response.getWorkflowState()).error(null).build();
                flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                    workflowId,
                    tenantId,
                    newState,
                    ActionListener.wrap(updateResponse -> {

                    }, exception -> {
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Failed to update workflow state: {}",
                            workflowId
                        ).getFormattedMessage();
                        logger.error(errorMessage, exception);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    })
                );
            }

            // Update State Index, maintain resources created for subsequent execution
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                tenantId,
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.PROVISIONING),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.IN_PROGRESS),
                    Map.entry(PROVISION_START_TIME_FIELD, Instant.now().toEpochMilli()),
                    Map.entry(RESOURCES_CREATED_FIELD, resourceCreated)
                ),
                ActionListener.wrap(updateResponse -> {

                    logger.info("Updated workflow {} state to {}", request.getWorkflowId(), State.PROVISIONING);

                    // Attach last provisioned time to updated template and execute reprovisioning
                    Template updatedTemplateWithProvisionedTime = Template.builder(updatedTemplate)
                        .lastProvisionedTime(Instant.now())
                        .build();
                    if (request.getWaitForCompletionTimeout() == TimeValue.MINUS_ONE) {
                        executeWorkflowAsync(workflowId, updatedTemplateWithProvisionedTime, reprovisionProcessSequence, listener);
                    } else {
                        executeWorkflowSync(
                            workflowId,
                            updatedTemplate,
                            reprovisionProcessSequence,
                            listener,
                            request.getWaitForCompletionTimeout().getMillis()
                        );
                    }

                    if (request.getWaitForCompletionTimeout() == TimeValue.MINUS_ONE) {
                        listener.onResponse(new WorkflowResponse(workflowId));
                    } else {
                        logger.info("Waiting for workflow completion");
                    }

                }, exception -> {
                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to update workflow state: {}", workflowId)
                        .getFormattedMessage();
                    logger.error(errorMessage, exception);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                })
            );
        }, exception -> {
            if (exception instanceof FlowFrameworkException) {
                listener.onFailure(exception);
            } else {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Failed to get workflow state for workflow {}",
                    workflowId
                ).getFormattedMessage();
                logger.error(errorMessage, exception);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
            }
        }));
    }

    /**
     * Retrieves a thread from the provision thread pool to execute a workflow
     * @param workflowId The id of the workflow
     * @param template The updated template to store upon successful execution
     * @param workflowSequence The sorted workflow to execute
     * @param listener ActionListener for any failures that don't get caught earlier in below step
     */
    private void executeWorkflowAsync(
        String workflowId,
        Template template,
        List<ProcessNode> workflowSequence,
        ActionListener<WorkflowResponse> listener
    ) {
        try {
            threadPool.executor(PROVISION_WORKFLOW_THREAD_POOL).execute(() -> {
                updateTemplate(template, workflowId);
                executeWorkflow(template, workflowSequence, workflowId, listener, false);
            });
        } catch (Exception exception) {
            listener.onFailure(new FlowFrameworkException("Failed to execute workflow " + workflowId, ExceptionsHelper.status(exception)));
        }
    }

    private void executeWorkflowSync(
        String workflowId,
        Template template,
        List<ProcessNode> workflowSequence,
        ActionListener<WorkflowResponse> listener,
        long timeout
    ) {
        AtomicBoolean isResponseSent = new AtomicBoolean(false);
        CompletableFuture.runAsync(() -> {
            try {
                updateTemplate(template, workflowId);
                executeWorkflow(template, workflowSequence, workflowId, new ActionListener<>() {
                    @Override
                    public void onResponse(WorkflowResponse workflowResponse) {
                        WorkflowTimeoutUtility.handleResponse(workflowId, workflowResponse, isResponseSent, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        WorkflowTimeoutUtility.handleFailure(workflowId, e, isResponseSent, listener);
                    }
                }, true);
            } catch (Exception ex) {
                WorkflowTimeoutUtility.handleFailure(workflowId, ex, isResponseSent, listener);
            }
        }, threadPool.executor(PROVISION_WORKFLOW_THREAD_POOL));
        WorkflowTimeoutUtility.scheduleTimeoutHandler(
            client,
            threadPool,
            workflowId,
            template.getTenantId(),
            listener,
            timeout,
            isResponseSent
        );
    }

    /**
     * Replace template document
     * @param template The template to store after reprovisioning completes successfully
     * @param workflowId The workflowId associated with the workflow that is executing
     */
    private void updateTemplate(Template template, String workflowId) {
        flowFrameworkIndicesHandler.updateTemplateInGlobalContext(workflowId, template, ActionListener.wrap(templateResponse -> {
            logger.info("Updated template for {}", workflowId);
        }, exception -> { logger.error("Failed to update use case template for {}", workflowId, exception); }),
            true  // ignores NOT_STARTED state if request is to reprovision
        );
    }

    /**
     * Executes the given workflow sequence
     * @param template The template to store after reprovisioning completes successfully
     * @param workflowSequence The topologically sorted workflow to execute
     * @param workflowId The workflowId associated with the workflow that is executing
     */
    private void executeWorkflow(
        Template template,
        List<ProcessNode> workflowSequence,
        String workflowId,
        ActionListener<WorkflowResponse> listener,
        boolean isSyncExecution
    ) {
        String currentStepId = "";
        try {
            Map<String, PlainActionFuture<?>> workflowFutureMap = new LinkedHashMap<>();
            for (ProcessNode processNode : workflowSequence) {
                List<ProcessNode> predecessors = processNode.predecessors();
                logger.info(
                    "Queueing Process [{} (type: {})].{}",
                    processNode.id(),
                    processNode.workflowStep().getName(),
                    predecessors.isEmpty()
                        ? " Can start immediately!"
                        : String.format(
                            Locale.getDefault(),
                            " Must wait for [%s] to complete first.",
                            predecessors.stream().map(p -> p.id()).collect(Collectors.joining(", "))
                        )
                );

                workflowFutureMap.put(processNode.id(), processNode.execute());
            }

            // Attempt to complete each workflow step future, may throw a ExecutionException if any step completes exceptionally
            for (Map.Entry<String, PlainActionFuture<?>> e : workflowFutureMap.entrySet()) {
                currentStepId = e.getKey();
                e.getValue().actionGet();
            }

            logger.info("Reprovisioning completed successfully for workflow {}", workflowId);
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                template.getTenantId(),
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.COMPLETED),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.DONE),
                    Map.entry(PROVISION_END_TIME_FIELD, Instant.now().toEpochMilli())
                ),
                ActionListener.wrap(updateResponse -> {

                    logger.info("updated workflow {} state to {}", workflowId, State.COMPLETED);
                    if (isSyncExecution) {
                        client.execute(
                            GetWorkflowStateAction.INSTANCE,
                            new GetWorkflowStateRequest(workflowId, false, template.getTenantId()),
                            ActionListener.wrap(response -> {
                                listener.onResponse(new WorkflowResponse(workflowId, response.getWorkflowState()));
                            }, exception -> {
                                String errorMessage = "Failed to get workflow state.";
                                logger.error(errorMessage, exception);
                                if (exception instanceof FlowFrameworkException) {
                                    listener.onFailure(exception);
                                } else {
                                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                                }
                            })
                        );
                    }
                }, exception -> { logger.error("Failed to update workflow state for workflow {}", workflowId, exception); })
            );
        } catch (Exception ex) {
            RestStatus status;
            if (ex instanceof FlowFrameworkException) {
                status = ((FlowFrameworkException) ex).getRestStatus();
            } else {
                status = ExceptionsHelper.status(ex);
            }
            logger.error("Reprovisioning failed for workflow {} during step {}.", workflowId, currentStepId, ex);
            String errorMessage = (ex.getCause() == null ? ex.getMessage() : ex.getCause().getClass().getName())
                + " during step "
                + currentStepId
                + ", restStatus: "
                + status.toString();
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                template.getTenantId(),
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.FAILED),
                    Map.entry(ERROR_FIELD, errorMessage),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.FAILED),
                    Map.entry(PROVISION_END_TIME_FIELD, Instant.now().toEpochMilli())
                ),
                ActionListener.wrap(updateResponse -> {
                    logger.info("updated workflow {} state to {}", workflowId, State.FAILED);
                }, exceptionState -> { logger.error("Failed to update workflow state for workflow {}", workflowId, exceptionState); })
            );
        }
    }

}
