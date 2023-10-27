/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

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
import org.opensearch.flowframework.workflow.CreateIndexStep;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlowFrameworkIndicesHandlerTests extends OpenSearchTestCase {
    @Mock
    private Client client;
    @Mock
    private CreateIndexStep createIndexStep;
    @Mock
    private ThreadPool threadPool;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private AdminClient adminClient;
    private IndicesAdminClient indicesAdminClient;
    private ThreadContext threadContext;
    @Mock
    protected ClusterService clusterService;
    @Mock
    private FlowFrameworkIndicesHandler flowMock;
    // private static final String META = "_meta";
    // private static final String SCHEMA_VERSION_FIELD = "schemaVersion";
    @Mock
    private Metadata metadata;
    // private Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();
    @Mock
    IndexMetadata indexMetadata;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        flowFrameworkIndicesHandler = new FlowFrameworkIndicesHandler(client, clusterService);
        adminClient = mock(AdminClient.class);
        indicesAdminClient = mock(IndicesAdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test cluster")).build());
        when(metadata.indices()).thenReturn(Map.of(GLOBAL_CONTEXT_INDEX, indexMetadata));
        when(adminClient.indices()).thenReturn(indicesAdminClient);
    }

    /*
    public void testPutTemplateToGlobalContext() throws IOException {
        Template template = mock(Template.class);
        when(template.toDocumentSource(any(XContentBuilder.class), eq(ToXContent.EMPTY_PARAMS))).thenAnswer(invocation -> {
            XContentBuilder builder = invocation.getArgument(0);
            return builder;
        });
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<Boolean> callback = invocation.getArgument(1);
            callback.onResponse(true);
            return null;
        }).when(flowMock).initFlowFrameworkIndexIfAbsent(any(FlowFrameworkIndex.class), any());

        flowFrameworkIndicesHandler.putTemplateToGlobalContext(template, listener);

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

        flowFrameworkIndicesHandler.storeResponseToGlobalContext(documentId, updatedFields, listener);

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

        flowFrameworkIndicesHandler.updateTemplateInGlobalContext("1", template, null);

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(client, times(1)).index(requestCaptor.capture(), any());

        assertEquals("1", requestCaptor.getValue().id());
    }

    public void testFailedUpdateTemplateInGlobalContext() throws IOException {
        Template template = mock(Template.class);
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        // when(createIndexStep.doesIndexExist(any())).thenReturn(false);

        flowFrameworkIndicesHandler.updateTemplateInGlobalContext("1", template, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);

        verify(listener, times(1)).onFailure(exceptionCaptor.capture());

        assertEquals(
            "Failed to update template for workflow_id : 1, global_context index does not exist.",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testInitIndexIfAbsent_IndexNotPresent() {
         when(metadata.hasIndex(FlowFrameworkIndex.GLOBAL_CONTEXT.getIndexName())).thenReturn(false);

         @SuppressWarnings("unchecked")
         ActionListener<Boolean> listener = mock(ActionListener.class);
         flowFrameworkIndicesHandler.initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);

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

        flowFrameworkIndicesHandler.initFlowFrameworkIndexIfAbsent(index, listener);

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

        flowFrameworkIndicesHandler.initFlowFrameworkIndexIfAbsent(index, listener);
        assertTrue(indexMappingUpdated.get(index.getIndexName()).get());
    }

    public void testDoesIndexExist() {
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetaData = mock(Metadata.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);

        flowFrameworkIndicesHandler.doesIndexExist(GLOBAL_CONTEXT_INDEX);

        ArgumentCaptor<String> indexExistsCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockMetaData, times(1)).hasIndex(indexExistsCaptor.capture());

        assertEquals(GLOBAL_CONTEXT_INDEX, indexExistsCaptor.getValue());
    }
    */
}
