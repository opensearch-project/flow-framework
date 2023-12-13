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
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchWorkflowTransportActionTests extends OpenSearchTestCase {

    private SearchWorkflowTransportAction searchWorkflowTransportAction;
    private Client client;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.client = mock(Client.class);

        this.searchWorkflowTransportAction = new SearchWorkflowTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client
        );

    }

    public void testFailedSearchWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        SearchRequest searchRequest = new SearchRequest();

        doAnswer(invocation -> {
            ActionListener<SearchResponse> responseListener = invocation.getArgument(2);
            responseListener.onFailure(new Exception("Search failed"));
            return null;
        }).when(client).search(searchRequest);

        searchWorkflowTransportAction.doExecute(mock(Task.class), searchRequest, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
    }

    public void testSearchWorkflow() {
        threadPool = mock(ThreadPool.class);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        SearchRequest searchRequest = new SearchRequest();

        searchWorkflowTransportAction.doExecute(mock(Task.class), searchRequest, listener);
        verify(client, times(1)).search(any(SearchRequest.class), any());
    }

}
