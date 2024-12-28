/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport.handler;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.impl.SdkClientFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.flowframework.TestHelpers.clusterSetting;
import static org.opensearch.flowframework.TestHelpers.matchAllRequest;
import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchHandlerTests extends OpenSearchTestCase {

    private static final TestThreadPool testThreadPool = spy(
        new TestThreadPool(
            SearchHandlerTests.class.getName(),
            new ScalingExecutorBuilder(
                WORKFLOW_THREAD_POOL,
                1,
                Math.max(2, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
                TimeValue.timeValueMinutes(1),
                FLOW_FRAMEWORK_THREAD_POOL_PREFIX + WORKFLOW_THREAD_POOL
            )
        )
    );

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

        when(client.threadPool()).thenReturn(testThreadPool);

        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(testThreadPool.getThreadContext()).thenReturn(threadContext);

        request = mock(SearchRequest.class);
        listener = mock(ActionListener.class);
    }

    @AfterClass
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    public void testSearchException() throws InterruptedException {
        doThrow(new RuntimeException("test")).when(client).search(any());

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<SearchResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        searchHandler.search(request, null, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        verify(listener, times(1)).onFailure(any());
    }

    public void testFilterEnabledWithWrongSearch() throws InterruptedException {
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class), null);

        searchHandler = new SearchHandler(settings, clusterService, client, sdkClient, FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<SearchResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        searchHandler.search(request, null, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        verify(listener, times(1)).onFailure(any());
    }

    public void testFilterEnabled() throws InterruptedException {
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        clusterService = new ClusterService(settings, clusterSettings, mock(ThreadPool.class), null);

        searchHandler = new SearchHandler(settings, clusterService, client, sdkClient, FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES);

        CountDownLatch latch = new CountDownLatch(1);
        LatchedActionListener<SearchResponse> latchedActionListener = new LatchedActionListener<>(listener, latch);
        searchHandler.search(matchAllRequest(), null, latchedActionListener);
        latch.await(1, TimeUnit.SECONDS);

        verify(client, times(1)).search(any());
    }

}
