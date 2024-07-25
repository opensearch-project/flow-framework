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
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.plugins.PluginsService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.ERROR_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_END_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_START_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.RESOURCES_CREATED_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;

/**
 * Transport Action to reprovision a provisioned template
 */
public class ReprovisionWorkflowTransportAction extends HandledTransportAction<ReprovisionWorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(ReprovisionWorkflowTransportAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final WorkflowStepFactory workflowStepFactory;
    private final WorkflowProcessSorter workflowProcessSorter;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private final PluginsService pluginsService;
    private final EncryptorUtils encryptorUtils;

    @Inject
    public ReprovisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        WorkflowStepFactory workflowStepFactory,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        EncryptorUtils encryptorUtils,
        PluginsService pluginsService
    ) {
        super(ReprovisionWorkflowAction.NAME, transportService, actionFilters, ReprovisionWorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.workflowStepFactory = workflowStepFactory;
        this.workflowProcessSorter = workflowProcessSorter;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
        this.encryptorUtils = encryptorUtils;
        this.pluginsService = pluginsService;
    }

    @Override
    protected void doExecute(Task task, ReprovisionWorkflowRequest request, ActionListener<WorkflowResponse> listener) {

        String workflowId = request.getWorkflowId();

        // Original template is retrieved from index, attempt to decrypt any exisiting credentials before processing
        Template originalTemplate = encryptorUtils.decryptTemplateCredentials(request.getOriginalTemplate());
        Template updatedTemplate = request.getUpdatedTemplate();

        // Validate updated template prior to execution
        Workflow provisionWorkflow = updatedTemplate.workflows().get(PROVISION_WORKFLOW);
        List<ProcessNode> updatedProcessSequence = workflowProcessSorter.sortProcessNodes(
            provisionWorkflow,
            request.getWorkflowId(),
            Collections.emptyMap() // TODO : Add suport to reprovision substitution templates
        );

        try {
            workflowProcessSorter.validate(updatedProcessSequence, pluginsService);
        } catch (Exception e) {
            String errormessage = "Workflow validation failed for workflow " + request.getWorkflowId();
            listener.onFailure(new FlowFrameworkException(errormessage, RestStatus.BAD_REQUEST));
        }

        // Retrieve resources created
        GetWorkflowStateRequest getStateRequest = new GetWorkflowStateRequest(workflowId, true);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            logger.info("Querying state for workflow: {}", workflowId);
            client.execute(GetWorkflowStateAction.INSTANCE, getStateRequest, ActionListener.wrap(response -> {
                context.restore();

                // Generate reprovision sequence
                List<ResourceCreated> resourceCreated = response.getWorkflowState().resourcesCreated();
                List<ProcessNode> reprovisionProcessSequence = workflowProcessSorter.createReprovisionSequence(
                    workflowId,
                    originalTemplate,
                    updatedTemplate,
                    resourceCreated
                );

                // Update State Index, maintain resources created for subsequent execution
                flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                    workflowId,
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
                        executeWorkflowAsync(workflowId, updatedTemplateWithProvisionedTime, reprovisionProcessSequence, listener);

                        listener.onResponse(new WorkflowResponse(workflowId));

                    }, exception -> {
                        String errorMessage = "Failed to update workflow state: " + workflowId;
                        logger.error(errorMessage, exception);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    })
                );
            }, exception -> {
                if (exception instanceof FlowFrameworkException) {
                    listener.onFailure(exception);
                } else {
                    String errorMessage = "Failed to get workflow state for workflow " + workflowId;
                    logger.error(errorMessage, exception);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }
            }));
        } catch (Exception e) {
            String errorMessage = "Failed to get workflow state for workflow " + workflowId;
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }

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
            threadPool.executor(PROVISION_WORKFLOW_THREAD_POOL).execute(() -> { executeWorkflow(template, workflowSequence, workflowId); });
        } catch (Exception exception) {
            listener.onFailure(new FlowFrameworkException("Failed to execute workflow " + workflowId, ExceptionsHelper.status(exception)));
        }
    }

    /**
     * Executes the given workflow sequence
     * @param template The template to store after reprovisioning completes successfully
     * @param workflowSequence The topologically sorted workflow to execute
     * @param workflowId The workflowId associated with the workflow that is executing
     */
    private void executeWorkflow(Template template, List<ProcessNode> workflowSequence, String workflowId) {
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

            logger.info("Reprovisioning completed successfully for workflow {}", workflowId);
            flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                workflowId,
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.COMPLETED),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.DONE),
                    Map.entry(PROVISION_END_TIME_FIELD, Instant.now().toEpochMilli())
                ),
                ActionListener.wrap(updateResponse -> {

                    logger.info("updated workflow {} state to {}", workflowId, State.COMPLETED);

                    // Replace template document
                    flowFrameworkIndicesHandler.updateTemplateInGlobalContext(
                        workflowId,
                        template,
                        ActionListener.wrap(templateResponse -> {
                            logger.info("Updated template for {}", workflowId, State.COMPLETED);
                        }, exception -> {
                            String errorMessage = "Failed to update use case template for " + workflowId;
                            logger.error(errorMessage, exception);
                        }),
                        true  // ignores NOT_STARTED state if request is to reprovision
                    );
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
