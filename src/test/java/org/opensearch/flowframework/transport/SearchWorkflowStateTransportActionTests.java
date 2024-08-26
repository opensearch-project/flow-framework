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
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.transport.handler.SearchHandler;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchWorkflowStateTransportActionTests extends OpenSearchTestCase {

    private SearchWorkflowStateTransportAction searchWorkflowStateTransportAction;
    private Client client;
    private ThreadPool threadPool;
    private ThreadContext threadContext;
    private SearchHandler searchHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        searchHandler = mock(SearchHandler.class);
        this.client = mock(Client.class);
        this.threadPool = mock(ThreadPool.class);
        this.threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        this.searchWorkflowStateTransportAction = new SearchWorkflowStateTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            searchHandler
        );

    }

    public void testSearchWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);

        doAnswer(invocation -> {
            SearchRequest request = invocation.getArgument(0);
            ActionListener<SearchResponse> responseListener = invocation.getArgument(1);
            ThreadContext.StoredContext storedContext = mock(ThreadContext.StoredContext.class);
            searchHandler.validateRole(request, null, responseListener, storedContext);
            responseListener.onResponse(mock(SearchResponse.class));
            return null;
        }).when(searchHandler).search(any(SearchRequest.class), any(ActionListener.class));

        doAnswer(invocation -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(mock(SearchResponse.class));
            return null;
        }).when(client).search(any(SearchRequest.class), any(ActionListener.class));

        searchWorkflowStateTransportAction.doExecute(mock(Task.class), searchRequest, listener);
        verify(searchHandler).search(any(SearchRequest.class), any(ActionListener.class));
    }

}
