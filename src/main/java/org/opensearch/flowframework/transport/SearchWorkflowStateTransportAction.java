/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport Action to search workflow states
 */
public class SearchWorkflowStateTransportAction extends HandledTransportAction<SearchRequest, SearchResponse> {

    private Client client;

    /**
     * Instantiates a new SearchWorkflowStateTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param client The client used to make the request to OS
     */
    @Inject
    public SearchWorkflowStateTransportAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(SearchWorkflowStateAction.NAME, transportService, actionFilters, SearchRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> actionListener) {
        // TODO: AccessController should take care of letting the user with right permission to view the workflow
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            client.search(request, ActionListener.runBefore(actionListener, context::restore));
        } catch (Exception e) {
            actionListener.onFailure(e);
        }
    }
}
