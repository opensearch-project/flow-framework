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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    public void testPutTemplateToGlobalContext() throws IOException {
        Template template = mock(Template.class);
        when(template.toXContent(any(XContentBuilder.class), eq(ToXContent.EMPTY_PARAMS))).thenAnswer(invocation -> {
            XContentBuilder builder = invocation.getArgument(0);
            return builder;
        });
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<Boolean> callback = invocation.getArgument(1);
            callback.onResponse(true);
            return null;
        }).when(createIndexStep).initIndexIfAbsent(any(FlowFrameworkIndex.class), any());

        globalContextHandler.putTemplateToGlobalContext(template, listener);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client, times(1)).index(requestCaptor.capture(), any());

        assertEquals(GLOBAL_CONTEXT_INDEX, requestCaptor.getValue().index());
    }

    public void testStoreResponseToGlobalContext() {
        String documentId = "docId";
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put("field1", "value1");
        @SuppressWarnings("unchecked")
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);

        globalContextHandler.storeResponseToGlobalContext(documentId, updatedFields, listener);

        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(client, times(1)).update(requestCaptor.capture(), any());

        assertEquals(GLOBAL_CONTEXT_INDEX, requestCaptor.getValue().index());
        assertEquals(documentId, requestCaptor.getValue().id());
    }

    public void testUpdateTemplateInGlobalContext() throws IOException {
        Template template = mock(Template.class);
        when(template.toXContent(any(XContentBuilder.class), eq(ToXContent.EMPTY_PARAMS))).thenAnswer(invocation -> {
            XContentBuilder builder = invocation.getArgument(0);
            return builder;
        });
        when(createIndexStep.doesIndexExist(any())).thenReturn(true);

        globalContextHandler.updateTemplateInGlobalContext("1", template, null);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client, times(1)).index(requestCaptor.capture(), any());

        assertEquals("1", requestCaptor.getValue().id());
    }

    public void testFailedUpdateTemplateInGlobalContext() throws IOException {
        Template template = mock(Template.class);
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        when(createIndexStep.doesIndexExist(any())).thenReturn(false);

        globalContextHandler.updateTemplateInGlobalContext("1", template, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());

        assertEquals(
            "Failed to update template for workflow_id : 1, global_context index does not exist.",
            exceptionCaptor.getValue().getMessage()
        );

    }
}
