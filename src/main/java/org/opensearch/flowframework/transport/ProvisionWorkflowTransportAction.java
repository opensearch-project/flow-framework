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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
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
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_END_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_START_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.RESOURCES_CREATED_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;

/**
 * Transport Action to provision a workflow from a stored use case template
 */
public class ProvisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(ProvisionWorkflowTransportAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final WorkflowProcessSorter workflowProcessSorter;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final EncryptorUtils encryptorUtils;
    private final PluginsService pluginsService;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param threadPool The OpenSearch thread pool
     * @param client The node client to retrieve a stored use case template
     * @param workflowProcessSorter Utility class to generate a togologically sorted list of Process nodes
     * @param flowFrameworkIndicesHandler Class to handle all internal system indices actions
     * @param encryptorUtils Utility class to handle encryption/decryption
     * @param pluginsService The Plugins Service
     */
    @Inject
    public ProvisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        EncryptorUtils encryptorUtils,
        PluginsService pluginsService
    ) {
        super(ProvisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.workflowProcessSorter = workflowProcessSorter;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.encryptorUtils = encryptorUtils;
        this.pluginsService = pluginsService;
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
                List<ProcessNode> provisionProcessSequence = workflowProcessSorter.sortProcessNodes(
                    provisionWorkflow,
                    workflowId,
                    request.getParams()
                );
                workflowProcessSorter.validate(provisionProcessSequence, pluginsService);

                flowFrameworkIndicesHandler.isWorkflowNotStarted(workflowId, workflowIsNotStarted -> {
                    if (workflowIsNotStarted) {
                        flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                            workflowId,
                            Map.ofEntries(
                                Map.entry(STATE_FIELD, State.PROVISIONING),
                                Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.IN_PROGRESS),
                                Map.entry(PROVISION_START_TIME_FIELD, Instant.now().toEpochMilli()),
                                Map.entry(RESOURCES_CREATED_FIELD, Collections.emptyList())
                            ),
                            ActionListener.wrap(updateResponse -> {
                                logger.info("updated workflow {} state to {}", request.getWorkflowId(), State.PROVISIONING);
                                executeWorkflowAsync(workflowId, provisionProcessSequence, listener);
                                listener.onResponse(new WorkflowResponse(workflowId));
                            }, exception -> {
                                String errorMessage = "Failed to update wowrfow state: " + workflowId;
                                logger.error(errorMessage);
                                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                            })
                        );
                    } else {
                        String errorMessage = "The template has already been provisioned: " + workflowId;
                        logger.error(errorMessage);
                        listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                    }
                }, listener);
            }, exception -> {
                if (exception instanceof FlowFrameworkException) {
                    logger.error("Workflow validation failed for workflow {}", workflowId);
                    listener.onFailure(exception);
                } else {
                    String errorMessage = "Failed to retrieve template from global context";
                    logger.error(errorMessage);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }
            }));
        } catch (Exception e) {
            String errorMessage = "Failed to retrieve template from global context";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

    /**
     * Retrieves a thread from the provision thread pool to execute a workflow
     * @param workflowId The id of the workflow
     * @param workflowSequence The sorted workflow to execute
     * @param listener ActionListener for any failures that don't get caught earlier in below step
     */
    private void executeWorkflowAsync(String workflowId, List<ProcessNode> workflowSequence, ActionListener<WorkflowResponse> listener) {
        try {
            threadPool.executor(PROVISION_WORKFLOW_THREAD_POOL).execute(() -> { executeWorkflow(workflowSequence, workflowId); });
        } catch (Exception exception) {
            listener.onFailure(new FlowFrameworkException("Failed to execute workflow " + workflowId, ExceptionsHelper.status(exception)));
        }
    }

    /**
     * Executes the given workflow sequence
     * @param workflowSequence The topologically sorted workflow to execute
     * @param workflowId The workflowId associated with the workflow that is executing
     */
    private void executeWorkflow(List<ProcessNode> workflowSequence, String workflowId) {
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
                Map.ofEntries(
                    Map.entry(STATE_FIELD, State.COMPLETED),
                    Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.DONE),
                    Map.entry(PROVISION_END_TIME_FIELD, Instant.now().toEpochMilli())
                ),
                ActionListener.wrap(updateResponse -> {
                    logger.info("updated workflow {} state to {}", workflowId, State.COMPLETED);
                }, exception -> { logger.error("Failed to update workflow state"); })
            );
        } catch (Exception ex) {
            logger.error("Provisioning failed for workflow {} during step {}.", workflowId, currentStepId);
            String errorMessage = (ex.getCause() == null ? ex.getClass().getName() : ex.getCause().getClass().getName())
                + " during step "
                + currentStepId;
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
                }, exceptionState -> { logger.error("Failed to update workflow state"); })
            );
        }
    }

}
