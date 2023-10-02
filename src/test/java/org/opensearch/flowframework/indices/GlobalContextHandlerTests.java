/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.workflow.CreateIndexStep;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.mockito.Mockito.*;

public class GlobalContextHandlerTests extends OpenSearchTestCase {
    @Mock
    private Client client;
    @Mock
    private CreateIndexStep createIndexStep;
    @Mock
    private ThreadPool threadPool;
    private GlobalContextHandler globalContextHandler;
    private AdminClient adminClient;
    private IndicesAdminClient indicesAdminClient;
    private ThreadContext threadContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        globalContextHandler = new GlobalContextHandler(client, createIndexStep);
        adminClient = mock(AdminClient.class);
        indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
    }

    @Test
    public void testPutTemplateToGlobalContext() throws IOException {
        Template template = mock(Template.class);
        when(template.toXContent(any(XContentBuilder.class), eq(ToXContent.EMPTY_PARAMS))).thenAnswer(invocation -> {
            XContentBuilder builder = invocation.getArgument(0);
            return builder;
        });
        ActionListener<IndexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<Boolean> callback = invocation.getArgument(1);
            callback.onResponse(true);
            return null;
        }).when(createIndexStep).initIndexIfAbsent(any(), any());

        globalContextHandler.putTemplateToGlobalContext(template, listener);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client).index(requestCaptor.capture(), any());

        assertEquals(GLOBAL_CONTEXT_INDEX, requestCaptor.getValue().index());
    }

    @Test
    public void testStoreResponseToGlobalContext() {
        String documentId = "docId";
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put("field1", "value1");
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);

        globalContextHandler.storeResponseToGlobalContext(documentId, updatedFields, listener);

        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(client).update(requestCaptor.capture(), any());

        assertEquals(GLOBAL_CONTEXT_INDEX, requestCaptor.getValue().index());
        assertEquals(documentId, requestCaptor.getValue().id());
    }
}
