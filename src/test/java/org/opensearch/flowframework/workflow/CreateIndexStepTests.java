/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.indices.FlowFrameworkIndex;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.*;
import static org.mockito.Mockito.*;

public class CreateIndexStepTests extends OpenSearchTestCase {

    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    private ClusterService clusterService;

    private Client client;

    private AdminClient adminClient;

    @Mock
    private IndicesAdminClient indicesAdminClient;
    private CreateIndexStep createIndexStep;
    private ThreadContext threadContext;
    @Mock
    private ThreadPool threadPool;
    @Mock
    IndexMetadata indexMetadata;
    private Metadata metadata;
    Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        inputData = new WorkflowData(Map.ofEntries(Map.entry("index-name", "demo"), Map.entry("type", "knn")));
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        metadata = mock(Metadata.class);
        Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);

        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test cluster")).build());
        when(metadata.indices()).thenReturn(Map.of(GLOBAL_CONTEXT_INDEX, indexMetadata));

        createIndexStep = new CreateIndexStep(clusterService, client);
        CreateIndexStep.indexMappingUpdated = indexMappingUpdated;

    }

    public void testCreateIndexStep() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<CreateIndexResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = createIndexStep.execute(List.of(inputData));
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new CreateIndexResponse(true, true, "demo"));

        assertTrue(future.isDone() && !future.isCompletedExceptionally());

        Map<String, Object> outputData = Map.of("index-name", "demo");
        assertEquals(outputData, future.get().getContent());

    }

    public void testCreateIndexStepFailure() throws ExecutionException, InterruptedException {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<CreateIndexResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = createIndexStep.execute(List.of(inputData));
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), actionListenerCaptor.capture());

        actionListenerCaptor.getValue().onFailure(new Exception("Failed to create an index"));

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof Exception);
        assertEquals("Failed to create an index", ex.getCause().getMessage());
    }

    public void testInitIndexIfAbsent_IndexNotPresent() {
        when(metadata.hasIndex(FlowFrameworkIndex.GLOBAL_CONTEXT.getIndexName())).thenReturn(false);

        ActionListener<Boolean> listener = mock(ActionListener.class);
        createIndexStep.initIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);

        verify(indicesAdminClient).create(any(), any());
    }

    // public void testInitIndexIfAbsent_IndexExist() {
    // FlowFrameworkIndex index = FlowFrameworkIndex.GLOBAL_CONTEXT;
    // indexMappingUpdated.put(index.getIndexName(), new AtomicBoolean(false));
    //
    // when(metadata.hasIndex(index.getIndexName())).thenReturn(true);
    // when(metadata.indices()).thenReturn(Map.of(index.getIndexName(), indexMetadata));
    //
    // // Mock that the mapping's version is outdated, old version < new version
    // when(indexMetadata.mapping()).thenReturn(new MappingMetadata(META, Map.of(SCHEMA_VERSION_FIELD, 0)));
    //
    // ActionListener<Boolean> listener = mock(ActionListener.class);
    // createIndexStep.initIndexIfAbsent(index, listener);
    //
    // ArgumentCaptor<PutMappingRequest> captor = ArgumentCaptor.forClass(PutMappingRequest.class);
    // verify(indicesAdminClient).putMapping(captor.capture());
    //
    // PutMappingRequest capturedRequest = captor.getValue();
    // assertEquals(index.getIndexName(), capturedRequest.indices()[0]);
    // }

}
