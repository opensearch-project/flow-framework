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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.FlowFrameworkPlugin.PROVISION_THREAD_POOL;

/**
 * Transport Action to provision a workflow from a stored use case template
 */
public class ProvisionWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(ProvisionWorkflowTransportAction.class);

    // TODO : Move to common values class, pending implementation
    /**
     * The name of the provision workflow within the use case template
     */
    private static final String PROVISION_WORKFLOW = "provision";

    private final ThreadPool threadPool;
    private final Client client;
    private final WorkflowProcessSorter workflowProcessSorter;

    /**
     * Instantiates a new ProvisionWorkflowTransportAction
     * @param transportService The TransportService
     * @param actionFilters action filters
     * @param threadPool The OpenSearch thread pool
     * @param client The node client to retrieve a stored use case template
     * @param workflowProcessSorter Utility class to generate a togologically sorted list of Process nodes
     */
    @Inject
    public ProvisionWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        WorkflowProcessSorter workflowProcessSorter
    ) {
        super(ProvisionWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.workflowProcessSorter = workflowProcessSorter;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {

        if (request.getWorkflowId() == null) {
            // Workflow provisioning from inline template, first parse and then index the given use case template
            client.execute(CreateWorkflowAction.INSTANCE, request, ActionListener.wrap(workflowResponse -> {
                String workflowId = workflowResponse.getWorkflowId();
                Template template = request.getTemplate();

                // TODO : Use node client to update state index to PROVISIONING, given workflowId

                listener.onResponse(new WorkflowResponse(workflowId));

                // Asychronously begin provision workflow excecution
                executeWorkflowAsync(workflowId, template.workflows().get(PROVISION_WORKFLOW));

            }, exception -> { listener.onFailure(exception); }));
        } else {
            // Use case template has been previously saved, retrieve entry and execute
            String workflowId = request.getWorkflowId();

            // TODO : Retrieve template from global context index using node client
            Template template = null; // temporary, remove later

            // TODO : use node client to update state index entry to PROVISIONING, given workflowId

            listener.onResponse(new WorkflowResponse(workflowId));
            executeWorkflowAsync(workflowId, template.workflows().get(PROVISION_WORKFLOW));
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
            provisionWorkflowListener.onFailure(exception);
        }
    }

    /**
     * Topologically sorts a given workflow into a sequence of ProcessNodes and executes the workflow
     * @param workflow The workflow to execute
     * @param workflowListener The listener that updates the status of a workflow execution
     */
    private void executeWorkflow(Workflow workflow, ActionListener<String> workflowListener) {

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
        try {
            // Attempt to join each workflow step future, may throw a CompletionException if any step completes exceptionally
            workflowFutureList.forEach(CompletableFuture::join);

            // TODO : Create State Index request with provisioning state, start time, end time, etc, pending implementation. String for now
            workflowListener.onResponse("READY");
        } catch (CancellationException | CompletionException ex) {
            workflowListener.onFailure(ex);
        }
    }

}
