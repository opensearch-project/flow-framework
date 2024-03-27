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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.plugins.PluginsService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.SEARCH_RESPONSE;

/**
 * Orchestrate transport action
 */
public class OrchestrateTransportAction extends HandledTransportAction<OrchestrateRequest, SearchResponse> {

    private final Logger logger = LogManager.getLogger(OrchestrateTransportAction.class);

    private final Client client;
    private final WorkflowProcessSorter workflowProcessSorter;
    private final PluginsService pluginsService;
    private final SearchPipelineService searchPipelineService;

    /**
     * Creates a new Orchestrate Transport Action instance
     * @param transportService the transport service
     * @param actionFilters action filters
     * @param threadPool the thread pool
     * @param client the opensearch client
     * @param workflowProcessSorter the workflow process sorter
     * @param pluginsService the plugins service
     * @param searchPipelineService the search pipeline service
     */
    @Inject
    public OrchestrateTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        WorkflowProcessSorter workflowProcessSorter,
        PluginsService pluginsService,
        SearchPipelineService searchPipelineService
    ) {
        super(OrchestrateAction.NAME, transportService, actionFilters, OrchestrateRequest::new);
        this.client = client;
        this.workflowProcessSorter = workflowProcessSorter;
        this.pluginsService = pluginsService;
        this.searchPipelineService = searchPipelineService;
    }

    @Override
    protected void doExecute(Task task, OrchestrateRequest request, ActionListener<SearchResponse> listener) {

        // Get Template
        String workflowId = request.getWorkflowId();
        GetRequest getRequest = new GetRequest(GLOBAL_CONTEXT_INDEX, workflowId);

        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            logger.info("Querying workflow from global context: {}", workflowId);
            client.get(getRequest, ActionListener.wrap(response -> {
                context.restore();

                if (!response.isExists()) {
                    String errorMessage = "Failed to retrieve template (" + workflowId + ") from global context.";
                    logger.error(errorMessage);
                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
                    return;
                }

                // Parse template from document source
                Template template = Template.parse(response.getSourceAsString());

                // Hacky way to get the search pipeline service into the workflow step factory
                workflowProcessSorter.updateWorkflowStepFactory(searchPipelineService);

                // Sort and validate graph
                Workflow searchWorkflow = template.workflows().get("search");
                List<ProcessNode> searchProcessSequence = workflowProcessSorter.sortProcessNodes(
                    searchWorkflow,
                    workflowId,
                    request.getUserInputs() // pass in request body conten to prcess nodes
                );
                workflowProcessSorter.validate(searchProcessSequence, pluginsService);

                // Execute the workflow
                String currentStepId = "";
                try {
                    Map<String, PlainActionFuture<?>> workflowFutureMap = new LinkedHashMap<>();
                    for (ProcessNode processNode : searchProcessSequence) {
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
                    // Additionally track each returned object of type search response and return the last one
                    SearchResponse searchResponse = null;
                    for (Map.Entry<String, PlainActionFuture<?>> entry : workflowFutureMap.entrySet()) {
                        currentStepId = entry.getKey();
                        WorkflowData result = (WorkflowData) entry.getValue().actionGet();
                        if (result.getContent().containsKey(SEARCH_RESPONSE)) {
                            searchResponse = (SearchResponse) result.getContent().get(SEARCH_RESPONSE);
                        }
                    }
                    if (searchResponse == null) {
                        listener.onFailure(
                            new FlowFrameworkException("The search workflow did not return a search response", RestStatus.BAD_REQUEST)
                        );
                    } else {
                        logger.info("Search completed successfully for workflow {}", workflowId);
                        listener.onResponse(searchResponse);
                    }
                } catch (Exception ex) {
                    RestStatus status;
                    if (ex instanceof FlowFrameworkException) {
                        status = ((FlowFrameworkException) ex).getRestStatus();
                    } else {
                        status = ExceptionsHelper.status(ex);
                    }
                    logger.error("Search failed for workflow {} during step {}.", workflowId, currentStepId, ex);
                    String errorMessage = (ex.getCause() == null ? ex.getClass().getName() : ex.getCause().getClass().getName())
                        + " during step "
                        + currentStepId
                        + ", restStatus: "
                        + status.toString();
                    listener.onFailure(new FlowFrameworkException(errorMessage, status));
                }
            }, exception -> {
                if (exception instanceof FlowFrameworkException) {
                    logger.error("Workflow validation failed for workflow {}", workflowId);
                    listener.onFailure(exception);
                } else {
                    String errorMessage = "Failed to retrieve template from global context for workflow " + workflowId;
                    logger.error(errorMessage, exception);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }
            }));
        } catch (Exception e) {
            String errorMessage = "Failed to retrieve template from global context for workflow " + workflowId;
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

}
