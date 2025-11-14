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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.handler.SearchHandler;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport Action to search workflow states
 */
public class SearchWorkflowStateTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private final Logger logger = LogManager.getLogger(SearchWorkflowStateTransportAction.class);

    private SearchHandler searchHandler;

    /**
     * Instantiates a new SearchWorkflowStateTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param searchHandler The SearchHandler
     */
    @Inject
    public SearchWorkflowStateTransportAction(TransportService transportService, ActionFilters actionFilters, SearchHandler searchHandler) {
        super(SearchWorkflowStateAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.searchHandler = searchHandler;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> actionListener) {
        try {
            // We used the SearchRequest preference field to convey a tenant id if any
            String tenantId = null;
            if (request.preference() != null) {
                tenantId = request.preference();
                request.preference(null);
            }
            searchHandler.search(request, tenantId, CommonValue.WORKFLOW_STATE_RESOURCE_TYPE, actionListener);
        } catch (Exception e) {
            String errorMessage = "Failed to search workflow states in global context";
            logger.error(errorMessage, e);
            actionListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }

    }
}
