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
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.indices.FlowFrameworkIndex;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class CreateIndexStepTests extends OpenSearchTestCase {

    private static final String META = "_meta";
    private static final String SCHEMA_VERSION_FIELD = "schemaVersion";

    private WorkflowData inputData = WorkflowData.EMPTY;
    private Client client;
    private AdminClient adminClient;
    private CreateIndexStep createIndexStep;
    private ThreadContext threadContext;
    private Metadata metadata;
    private Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();

    @Mock
    private ClusterService clusterService;
    @Mock
    private IndicesAdminClient indicesAdminClient;
    @Mock
    private ThreadPool threadPool;
    @Mock
    IndexMetadata indexMetadata;

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
        @SuppressWarnings({ "unchecked" })
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
        @SuppressWarnings({ "unchecked" })
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

        @SuppressWarnings("unchecked")
        ActionListener<Boolean> listener = mock(ActionListener.class);
        createIndexStep.initIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);

        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), any());
    }

    public void testInitIndexIfAbsent_IndexExist() {
        FlowFrameworkIndex index = FlowFrameworkIndex.GLOBAL_CONTEXT;
        indexMappingUpdated.put(index.getIndexName(), new AtomicBoolean(false));

        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.hasIndex(index.getIndexName())).thenReturn(true);
        @SuppressWarnings("unchecked")
        ActionListener<Boolean> listener = mock(ActionListener.class);

        IndexMetadata mockIndexMetadata = mock(IndexMetadata.class);
        @SuppressWarnings("unchecked")
        Map<String, IndexMetadata> mockIndices = mock(Map.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.getMetadata()).thenReturn(mockMetadata);
        when(mockMetadata.indices()).thenReturn(mockIndices);
        when(mockIndices.get(anyString())).thenReturn(mockIndexMetadata);
        Map<String, Object> mockMapping = new HashMap<>();
        Map<String, Object> mockMetaMapping = new HashMap<>();
        mockMetaMapping.put(SCHEMA_VERSION_FIELD, 1);
        mockMapping.put(META, mockMetaMapping);
        MappingMetadata mockMappingMetadata = mock(MappingMetadata.class);
        when(mockIndexMetadata.mapping()).thenReturn(mockMappingMetadata);
        when(mockMappingMetadata.getSourceAsMap()).thenReturn(mockMapping);

        createIndexStep.initIndexIfAbsent(index, listener);

        ArgumentCaptor<PutMappingRequest> putMappingRequestArgumentCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);
        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<ActionListener<AcknowledgedResponse>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(indicesAdminClient, times(1)).putMapping(putMappingRequestArgumentCaptor.capture(), listenerCaptor.capture());
        PutMappingRequest capturedRequest = putMappingRequestArgumentCaptor.getValue();
        assertEquals(index.getIndexName(), capturedRequest.indices()[0]);
    }

    public void testInitIndexIfAbsent_IndexExist_returnFalse() {
        FlowFrameworkIndex index = FlowFrameworkIndex.GLOBAL_CONTEXT;
        indexMappingUpdated.put(index.getIndexName(), new AtomicBoolean(false));

        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.hasIndex(index.getIndexName())).thenReturn(true);

        @SuppressWarnings("unchecked")
        ActionListener<Boolean> listener = mock(ActionListener.class);
        @SuppressWarnings("unchecked")
        Map<String, IndexMetadata> mockIndices = mock(Map.class);
        when(mockClusterState.getMetadata()).thenReturn(mockMetadata);
        when(mockMetadata.indices()).thenReturn(mockIndices);
        when(mockIndices.get(anyString())).thenReturn(null);

        createIndexStep.initIndexIfAbsent(index, listener);
        assertTrue(indexMappingUpdated.get(index.getIndexName()).get());
    }

    public void testDoesIndexExist() {
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetaData = mock(Metadata.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);

        createIndexStep.doesIndexExist(GLOBAL_CONTEXT_INDEX);

        ArgumentCaptor<String> indexExistsCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMetaData, times(1)).hasIndex(indexExistsCaptor.capture());

        assertEquals(GLOBAL_CONTEXT_INDEX, indexExistsCaptor.getValue());
    }
}
