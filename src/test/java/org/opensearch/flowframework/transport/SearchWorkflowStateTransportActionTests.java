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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchWorkflowStateTransportActionTests extends OpenSearchTestCase {

    private SearchWorkflowStateTransportAction searchWorkflowStateTransportAction;
    private Client client;
    private ThreadPool threadPool;
    private ThreadContext threadContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.client = mock(Client.class);
        this.threadPool = mock(ThreadPool.class);
        this.threadContext = new ThreadContext(Settings.EMPTY);

        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        this.searchWorkflowStateTransportAction = new SearchWorkflowStateTransportAction(
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
            ActionListener<SearchResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Search failed"));
            return null;
        }).when(client).search(any(), any());

        searchWorkflowStateTransportAction.doExecute(mock(Task.class), searchRequest, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testSearchWorkflow() {
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        SearchRequest searchRequest = new SearchRequest();

        searchWorkflowStateTransportAction.doExecute(mock(Task.class), searchRequest, listener);
        verify(client, times(1)).search(any(SearchRequest.class), any());
    }

}
