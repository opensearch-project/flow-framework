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
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.util.TenantAwareHelper;
import org.opensearch.flowframework.util.WorkflowTimeoutUtility;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.plugins.PluginsService;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.tasks.Task;
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
 * Transport Action to provision a workflow from a stored use case template
 */
public class ProvisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(ProvisionWorkflowTransportAction.class);

    private final Client client;
    private final SdkClient sdkClient;
    private final WorkflowProcessSorter workflowProcessSorter;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private final EncryptorUtils encryptorUtils;
    private final PluginsService pluginsService;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param client The node client to retrieve a stored use case template
     * @param sdkClient the Multitenant Client
     * @param workflowProcessSorter Utility class to generate a togologically sorted list of Process nodes
     * @param flowFrameworkIndicesHandler Class to handle all internal system indices actions
     * @param flowFrameworkSettings The Flow Framework settings
     * @param encryptorUtils Utility class to handle encryption/decryption
     * @param pluginsService The Plugins Service
     * @param clusterService the cluster service
     * @param xContentRegistry the named content registry
     * @param settings the plugin settings
     */
    @Inject
    public ProvisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        SdkClient sdkClient,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        EncryptorUtils encryptorUtils,
        PluginsService pluginsService,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings
    ) {
        super(ProvisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.client = client;
        this.sdkClient = sdkClient;
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
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {
        // Retrieve use case template from global context
        String tenantId = request.getTemplate() == null ? null : request.getTemplate().getTenantId();
        if (!TenantAwareHelper.validateTenantId(flowFrameworkSettings.isMultiTenancyEnabled(), tenantId, listener)) {
            return;
        }
        String workflowId = request.getWorkflowId();
        User user = getUserContext(client);

        // Stash thread context to interact with system index
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {

            resolveUserAndExecute(
                user,
                workflowId,
                tenantId,
                filterByEnabled,
                false,
                flowFrameworkSettings.isMultiTenancyEnabled(),
                listener,
                () -> executeProvisionRequest(request, tenantId, listener, context),
                client,
                sdkClient,
                clusterService,
                xContentRegistry
            );
        } catch (Exception e) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to retrieve template from global context for workflow {}",
                workflowId
            ).getFormattedMessage();
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

    /**
     * Execute the provision request
     * 1. Retrieve template from global context
     * 2. Decrypt template
     * 3. Sort and validate graph
     * 4. Update state index
     * 5. Execute workflow asynchronously
     * 6. Update last provisioned field in template
     * 7. Return response
     * @param request the workflow request
     * @param tenantId
     * @param listener the action listener
     * @param context the thread context
     */
    private void executeProvisionRequest(
        WorkflowRequest request,
        String tenantId,
        ActionListener<WorkflowResponse> listener,
        ThreadContext.StoredContext context
    ) {
        String workflowId = request.getWorkflowId();
        logger.info("Querying workflow from global context: {}", workflowId);
        flowFrameworkIndicesHandler.getTemplate(workflowId, tenantId, ActionListener.wrap(response -> {
            context.restore();

            if (!response.isExists()) {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Failed to retrieve template ({}) from global context.",
                    workflowId
                ).getFormattedMessage();
                logger.error(errorMessage);
                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
                return;
            }

            // Parse template from document source
            Template parsedTemplate = Template.parse(response.getSourceAsString());

            // Decrypt template
            final Template template = encryptorUtils.decryptTemplateCredentials(parsedTemplate);

            // Sort and validate graph
            Workflow provisionWorkflow = template.workflows().get(PROVISION_WORKFLOW);
            List<ProcessNode> provisionProcessSequence = workflowProcessSorter.sortProcessNodes(
                provisionWorkflow,
                workflowId,
                request.getParams(),
                tenantId
            );
            workflowProcessSorter.validate(provisionProcessSequence, pluginsService);

            flowFrameworkIndicesHandler.getProvisioningProgress(workflowId, tenantId, progress -> {
                if (ProvisioningProgress.NOT_STARTED.equals(progress.orElse(null))) {
                    // update state index
                    flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                        workflowId,
                        tenantId,
                        Map.ofEntries(
                            Map.entry(STATE_FIELD, State.PROVISIONING),
                            Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.IN_PROGRESS),
                            Map.entry(PROVISION_START_TIME_FIELD, Instant.now().toEpochMilli()),
                            Map.entry(RESOURCES_CREATED_FIELD, Collections.emptyList())
                        ),
                        ActionListener.wrap(updateResponse -> {
                            logger.info("updated workflow {} state to {}", request.getWorkflowId(), State.PROVISIONING);
                            if (request.getWaitForCompletionTimeout() == TimeValue.MINUS_ONE) {
                                executeWorkflowAsync(workflowId, tenantId, provisionProcessSequence, listener);
                            } else {
                                executeWorkflowSync(
                                    workflowId,
                                    tenantId,
                                    provisionProcessSequence,
                                    listener,
                                    request.getWaitForCompletionTimeout().getMillis()
                                );
                            }
                            // update last provisioned field in template
                            Template newTemplate = Template.builder(template).lastProvisionedTime(Instant.now()).build();
                            flowFrameworkIndicesHandler.updateTemplateInGlobalContext(
                                request.getWorkflowId(),
                                newTemplate,
                                ActionListener.wrap(templateResponse -> {
                                    if (request.getWaitForCompletionTimeout() == TimeValue.MINUS_ONE) {
                                        listener.onResponse(new WorkflowResponse(request.getWorkflowId()));
                                    } else {
                                        logger.info("Waiting for workflow completion");
                                    }
                                }, exception -> {
                                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                        "Failed to update use case template {}",
                                        request.getWorkflowId()
                                    ).getFormattedMessage();
                                    logger.error(errorMessage, exception);
                                    if (exception instanceof FlowFrameworkException) {
                                        listener.onFailure(exception);
                                    } else {
                                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                                    }
                                }),
                                // We've already checked workflow is not started, ignore second check
                                true
                            );
                        }, exception -> {
                            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                "Failed to update workflow state: {}",
                                workflowId
                            ).getFormattedMessage();
                            logger.error(errorMessage, exception);
                            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                        })
                    );
                } else {
                    String errorMessage = "The workflow provisioning state is "
                        + (progress.isPresent() ? progress.get().toString() : "unknown")
                        + " and can not be provisioned unless its state is NOT_STARTED: "
                        + workflowId
                        + ". Deprovision the workflow to reset the state.";
                    logger.info(errorMessage);
                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                }
            }, listener);
        }, exception -> {
            if (exception instanceof FlowFrameworkException) {
                logger.error("Workflow validation failed for workflow {}", workflowId);
                listener.onFailure(exception);
            } else {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Failed to retrieve template from global context for workflow {}",
                    workflowId
                ).getFormattedMessage();
                logger.error(errorMessage, exception);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
            }
        }), context);
    }

    /**
     * Retrieves a thread from the provision thread pool to execute a workflow
     * @param workflowId The id of the workflow
     * @param workflowSequence The sorted workflow to execute
     * @param listener ActionListener for any failures that don't get caught earlier in below step
     */
    private void executeWorkflowAsync(
        String workflowId,
        String tenantId,
        List<ProcessNode> workflowSequence,
        ActionListener<WorkflowResponse> listener
    ) {
        try {
            client.threadPool().executor(PROVISION_WORKFLOW_THREAD_POOL).execute(() -> {
                executeWorkflow(workflowSequence, workflowId, tenantId, listener, false);
            });
        } catch (Exception exception) {
            listener.onFailure(new FlowFrameworkException("Failed to execute workflow " + workflowId, ExceptionsHelper.status(exception)));
        }
    }

    /**
     * Retrieves a thread from the provision thread pool to execute a workflow with a timeout mechanism.
     * If the execution exceeds the specified timeout, it will return the current status of the workflow.
     *
     * @param workflowId The id of the workflow
     * @param workflowSequence The sorted workflow to execute
     * @param listener ActionListener for any failures or responses
     * @param timeout The timeout duration in milliseconds
     */
    private void executeWorkflowSync(
        String workflowId,
        String tenantId,
        List<ProcessNode> workflowSequence,
        ActionListener<WorkflowResponse> listener,
        long timeout
    ) {
        AtomicBoolean isResponseSent = new AtomicBoolean(false);

        CompletableFuture.runAsync(() -> {
            try {
                executeWorkflow(workflowSequence, workflowId, tenantId, new ActionListener<>() {
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
        }, client.threadPool().executor(PROVISION_WORKFLOW_THREAD_POOL));

        WorkflowTimeoutUtility.scheduleTimeoutHandler(client, client.threadPool(), workflowId, tenantId, listener, timeout, isResponseSent);
    }

    /**
     * Executes the given workflow sequence
     * @param workflowSequence The topologically sorted workflow to execute
     * @param workflowId The workflowId associated with the workflow that is executing
     * @param tenantId The tenant id
     * @param listener The ActionListener to handle the workflow response or failure
     * @param isSyncExecution Flag indicating whether the workflow should be executed synchronously (true) or asynchronously (false)
     */
    private void executeWorkflow(
        List<ProcessNode> workflowSequence,
        String workflowId,
        String tenantId,
        ActionListener<WorkflowResponse> listener,
        boolean isSyncExecution
    ) {
        String currentStepId = "";
        try {
            Map<String, PlainActionFuture<?>> workflowFutureMap = new LinkedHashMap<>();
            for (ProcessNode processNode : workflowSequence) {
                List<ProcessNode> predecessors = processNode.predecessors();

                logger.info(
                    "Queueing process [{}].{}",
                    processNode.id(),
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

            logger.info("Provisioning completed successfully for workflow {}", workflowId);
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                tenantId,
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
                            new GetWorkflowStateRequest(workflowId, false, tenantId),
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
            logger.error("Provisioning failed for workflow {} during step {}.", workflowId, currentStepId, ex);
            String errorMessage = (ex.getCause() == null ? ex.getMessage() : ex.getCause().getClass().getName())
                + " during step "
                + currentStepId
                + ", restStatus: "
                + status.toString();
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                tenantId,
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
