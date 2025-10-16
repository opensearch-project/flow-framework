/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport.handler;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.junit.Before;

import java.util.Collections;

import static org.opensearch.flowframework.TestHelpers.clusterSetting;
import static org.opensearch.flowframework.TestHelpers.matchAllRequest;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchHandlerTests extends OpenSearchTestCase {

    private Client client;
    private SdkClient sdkClient;
    private Settings settings;
    private ClusterService clusterService;
    private SearchHandler searchHandler;
    private ClusterSettings clusterSettings;

    private SearchRequest request;

    private ActionListener<SearchResponse> listener;

    @SuppressWarnings("unchecked")
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), false).build();
        clusterSettings = clusterSetting(settings, FILTER_BY_BACKEND_ROLES);
        clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class), null);
        client = mock(Client.class);
        sdkClient = SdkClientFactory.createSdkClient(client, NamedXContentRegistry.EMPTY, Collections.emptyMap());
        searchHandler = new SearchHandler(settings, clusterService, client, sdkClient, FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES);

        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(client.threadPool().getThreadContext()).thenReturn(threadContext);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        request = mock(SearchRequest.class);
        listener = mock(ActionListener.class);
    }

    public void testSearchException() {
        doThrow(new RuntimeException("test")).when(client).search(any(), any());
        searchHandler.search(request, null, CommonValue.WORKFLOW_RESOURCE_TYPE, listener);

        verify(listener, times(1)).onFailure(any());
    }

    public void testFilterEnabledWithWrongSearch() {
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class), null);

        searchHandler = new SearchHandler(settings, clusterService, client, sdkClient, FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES);

        searchHandler.search(request, null, CommonValue.WORKFLOW_RESOURCE_TYPE, listener);

        verify(listener, times(1)).onFailure(any());
    }

    public void testFilterEnabled() {
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class), null);

        searchHandler = new SearchHandler(settings, clusterService, client, sdkClient, FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES);

        searchHandler.search(matchAllRequest(), null, CommonValue.WORKFLOW_RESOURCE_TYPE, listener);

        verify(client, times(1)).search(any(), any());
    }
}
