/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_START_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;

/**
 * Transport Action to provision a workflow from a stored use case template
 */
public class ProvisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(ProvisionWorkflowTransportAction.class);

    private final ThreadPool threadPool;
    private final Client client;
    private final WorkflowProcessSorter workflowProcessSorter;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param threadPool The OpenSearch thread pool
     * @param client The node client to retrieve a stored use case template
     * @param workflowProcessSorter Utility class to generate a togologically sorted list of Process nodes
     * @param flowFrameworkIndicesHandler Class to handle all internal system indices actions
     */
    @Inject
    public ProvisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler
    ) {
        super(ProvisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.workflowProcessSorter = workflowProcessSorter;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
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

                flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                    WORKFLOW_STATE_INDEX,
                    workflowId,
                    ImmutableMap.of(
                        STATE_FIELD,
                        State.PROVISIONING,
                        PROVISIONING_PROGRESS_FIELD,
                        ProvisioningProgress.IN_PROGRESS,
                        PROVISION_START_TIME_FIELD,
                        Instant.now().toEpochMilli()
                    ),
                    ActionListener.wrap(updateResponse -> {
                        logger.info("updated workflow {} state to PROVISIONING", request.getWorkflowId());
                    }, exception -> { logger.error("Failed to update workflow state : {}", exception.getMessage()); })
                );

                // Respond to rest action then execute provisioning workflow async
                listener.onResponse(new WorkflowResponse(workflowId));
                executeWorkflowAsync(workflowId, template.workflows().get(PROVISION_WORKFLOW));
            }, exception -> {
                logger.error("Failed to retrieve template from global context.", exception);
                listener.onFailure(new FlowFrameworkException(exception.getMessage(), RestStatus.INTERNAL_SERVER_ERROR));
            }));
        } catch (Exception e) {
            logger.error("Failed to retrieve template from global context.", e);
            listener.onFailure(new FlowFrameworkException(e.getMessage(), RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Retrieves a thread from the provision thread pool to execute a workflow
     * @param workflowId The id of the workflow
     * @param workflow The workflow to execute
     */
    private void executeWorkflowAsync(String workflowId, Workflow workflow) {
        // TODO : Update Action listener type to State index Request
        ActionListener<String> provisionWorkflowListener = ActionListener.wrap(response -> {
            logger.info("Provisioning completed successuflly for workflow {}", workflowId);

            // TODO : Create State index request to update STATE entry status to READY
        }, exception -> {
            logger.error("Provisioning failed for workflow {} : {}", workflowId, exception);

            // TODO : Create State index request to update STATE entry status to FAILED
        });
        try {
            threadPool.executor(PROVISION_THREAD_POOL).execute(() -> { executeWorkflow(workflow, provisionWorkflowListener); });
        } catch (Exception exception) {
            provisionWorkflowListener.onFailure(new FlowFrameworkException(exception.getMessage(), RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Topologically sorts a given workflow into a sequence of ProcessNodes and executes the workflow
     * @param workflow The workflow to execute
     * @param workflowListener The listener that updates the status of a workflow execution
     */
    private void executeWorkflow(Workflow workflow, ActionListener<String> workflowListener) {
        try {

            // Attempt to topologically sort the workflow graph
            List<ProcessNode> processSequence = workflowProcessSorter.sortProcessNodes(workflow);
            List<CompletableFuture<?>> workflowFutureList = new ArrayList<>();

            for (ProcessNode processNode : processSequence) {
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

                workflowFutureList.add(processNode.execute());
            }

            // Attempt to join each workflow step future, may throw a CompletionException if any step completes exceptionally
            workflowFutureList.forEach(CompletableFuture::join);

            // TODO : Create State Index request with provisioning state, start time, end time, etc, pending implementation. String for now
            workflowListener.onResponse("READY");

        } catch (IllegalArgumentException e) {
            workflowListener.onFailure(new FlowFrameworkException(e.getMessage(), RestStatus.BAD_REQUEST));
        } catch (CancellationException | CompletionException ex) {
            workflowListener.onFailure(new FlowFrameworkException(ex.getMessage(), RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

}
