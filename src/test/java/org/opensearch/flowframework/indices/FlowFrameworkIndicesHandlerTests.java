/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import org.opensearch.Version;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.workflow.CreateIndexStep;
import org.opensearch.index.get.GetResult;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class FlowFrameworkIndicesHandlerTests extends OpenSearchTestCase {
    @Mock
    private Client client;
    @Mock
    private CreateIndexStep createIndexStep;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private EncryptorUtils encryptorUtils;
    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private AdminClient adminClient;
    private IndicesAdminClient indicesAdminClient;
    private ThreadContext threadContext;
    @Mock
    protected ClusterService clusterService;
    @Mock
    private FlowFrameworkIndicesHandler flowMock;
    private static final String META = "_meta";
    private static final String SCHEMA_VERSION_FIELD = "schemaVersion";
    private Metadata metadata;
    private Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();
    @Mock
    IndexMetadata indexMetadata;
    private Template template;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        flowFrameworkIndicesHandler = new FlowFrameworkIndicesHandler(client, clusterService, encryptorUtils, xContentRegistry());
        adminClient = mock(AdminClient.class);
        indicesAdminClient = mock(IndicesAdminClient.class);
        metadata = mock(Metadata.class);

        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test cluster")).build());
        when(metadata.indices()).thenReturn(Map.of(GLOBAL_CONTEXT_INDEX, indexMetadata));

        Workflow workflow = TestHelpers.createSampleWorkflow();
        Version templateVersion = Version.fromString("1.0.0");
        List<Version> compatibilityVersions = List.of(Version.fromString("2.0.0"), Version.fromString("3.0.0"));
        this.template = new Template(
            "test",
            "description",
            "use case",
            templateVersion,
            compatibilityVersions,
            Map.of("workflow", workflow),
            Collections.emptyMap(),
            TestHelpers.randomUser(),
            null,
            null,
            null
        );
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

    public void testFailedUpdateTemplateInGlobalContextNotExisting() throws IOException {
        Template template = mock(Template.class);
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        FlowFrameworkIndex index = FlowFrameworkIndex.GLOBAL_CONTEXT;
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockMetadata.hasIndex(index.getIndexName())).thenReturn(true);
        when(flowFrameworkIndicesHandler.doesIndexExist(GLOBAL_CONTEXT_INDEX)).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to get template"));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        flowFrameworkIndicesHandler.updateTemplateInGlobalContext("1", template, listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Failed to get template"));
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
        FlowFrameworkIndex index = FlowFrameworkIndex.WORKFLOW_STATE;
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
        assertFalse(indexMappingUpdated.get(index.getIndexName()).get());
    }

    public void testInitIndexIfAbsent_IndexNotPresent() {
        when(metadata.hasIndex(FlowFrameworkIndex.GLOBAL_CONTEXT.getIndexName())).thenReturn(false);

        @SuppressWarnings("unchecked")
        ActionListener<Boolean> listener = mock(ActionListener.class);
        flowFrameworkIndicesHandler.initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);

        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), any());
    }

    public void testIsWorkflowProvisionedFailedParsing() {
        String documentId = randomAlphaOfLength(5);
        @SuppressWarnings("unchecked")
        Consumer<Optional<ProvisioningProgress>> function = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            // workFlowState.toXContent(builder, null);
            this.template.toXContent(builder, null);
            BytesReference workflowBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(WORKFLOW_STATE_INDEX, documentId, 1, 1, 1, true, workflowBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());
        flowFrameworkIndicesHandler.getProvisioningProgress(documentId, function, listener);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("Failed to parse workflow state"));
    }

    public void testCanDeleteWorkflowStateDoc() {
        String documentId = randomAlphaOfLength(5);
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        WorkflowState workFlowState = new WorkflowState(
            documentId,
            "test",
            "PROVISIONING",
            "NOT_STARTED",
            Instant.now(),
            Instant.now(),
            TestHelpers.randomUser(),
            Collections.emptyMap(),
            Collections.emptyList()
        );
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            workFlowState.toXContent(builder, null);
            BytesReference workflowBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(WORKFLOW_STATE_INDEX, documentId, 1, 1, 1, true, workflowBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        flowFrameworkIndicesHandler.canDeleteWorkflowStateDoc(documentId, false, canDelete -> { assertTrue(canDelete); }, listener);
    }

    public void testCanNotDeleteWorkflowStateDocInProgress() {
        String documentId = randomAlphaOfLength(5);
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        WorkflowState workFlowState = new WorkflowState(
            documentId,
            "test",
            "PROVISIONING",
            "IN_PROGRESS",
            Instant.now(),
            Instant.now(),
            TestHelpers.randomUser(),
            Collections.emptyMap(),
            Collections.emptyList()
        );
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            workFlowState.toXContent(builder, null);
            BytesReference workflowBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(WORKFLOW_STATE_INDEX, documentId, 1, 1, 1, true, workflowBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        flowFrameworkIndicesHandler.canDeleteWorkflowStateDoc(documentId, true, canDelete -> { assertFalse(canDelete); }, listener);
    }

    public void testDeleteWorkflowStateDocResourcesExist() {
        String documentId = randomAlphaOfLength(5);
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        WorkflowState workFlowState = new WorkflowState(
            documentId,
            "test",
            "PROVISIONING",
            "DONE",
            Instant.now(),
            Instant.now(),
            TestHelpers.randomUser(),
            Collections.emptyMap(),
            List.of(new ResourceCreated("w", "x", "y", "z"))
        );
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            workFlowState.toXContent(builder, null);
            BytesReference workflowBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(WORKFLOW_STATE_INDEX, documentId, 1, 1, 1, true, workflowBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        // Can't delete because resources exist
        flowFrameworkIndicesHandler.canDeleteWorkflowStateDoc(documentId, false, canDelete -> { assertFalse(canDelete); }, listener);

        // But can delete if clearStatus set true
        flowFrameworkIndicesHandler.canDeleteWorkflowStateDoc(documentId, true, canDelete -> { assertTrue(canDelete); }, listener);
    }

    public void testDoesTemplateExist() {
        String documentId = randomAlphaOfLength(5);
        @SuppressWarnings("unchecked")
        Consumer<Boolean> function = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);

            XContentBuilder builder = XContentFactory.jsonBuilder();
            this.template.toXContent(builder, null);
            BytesReference templateBytesRef = BytesReference.bytes(builder);
            GetResult getResult = new GetResult(GLOBAL_CONTEXT_INDEX, documentId, 1, 1, 1, true, templateBytesRef, null, null);
            responseListener.onResponse(new GetResponse(getResult));
            return null;
        }).when(client).get(any(GetRequest.class), any());
        flowFrameworkIndicesHandler.doesTemplateExist(documentId, function, listener);
        verify(function).accept(true);
    }

    public void testUpdateFlowFrameworkSystemIndexDoc() throws IOException {
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetaData = mock(Metadata.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);
        when(mockMetaData.hasIndex(WORKFLOW_STATE_INDEX)).thenReturn(true);

        @SuppressWarnings("unchecked")
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);

        // test success
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, Result.UPDATED));
            return null;
        }).when(client).update(any(UpdateRequest.class), any());

        flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc("1", Map.of("foo", "bar"), listener);

        ArgumentCaptor<UpdateResponse> responseCaptor = ArgumentCaptor.forClass(UpdateResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(Result.UPDATED, responseCaptor.getValue().getResult());

        // test failure
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to update state"));
            return null;
        }).when(client).update(any(UpdateRequest.class), any());

        flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc("1", Map.of("foo", "bar"), listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to update state", exceptionCaptor.getValue().getMessage());

        // test no index
        when(mockMetaData.hasIndex(WORKFLOW_STATE_INDEX)).thenReturn(false);
        flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc("1", Map.of("foo", "bar"), listener);

        verify(listener, times(2)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "Failed to update document 1 due to missing .plugins-flow-framework-state index",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testDeleteFlowFrameworkSystemIndexDoc() throws IOException {
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetaData = mock(Metadata.class);
        when(clusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);
        when(mockMetaData.hasIndex(WORKFLOW_STATE_INDEX)).thenReturn(true);

        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);

        // test success
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> responseListener = invocation.getArgument(1);
            responseListener.onResponse(new DeleteResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, true));
            return null;
        }).when(client).delete(any(DeleteRequest.class), any());

        flowFrameworkIndicesHandler.deleteFlowFrameworkSystemIndexDoc("1", listener);

        ArgumentCaptor<DeleteResponse> responseCaptor = ArgumentCaptor.forClass(DeleteResponse.class);
        verify(listener, times(1)).onResponse(responseCaptor.capture());
        assertEquals(Result.DELETED, responseCaptor.getValue().getResult());

        // test failure
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> responseListener = invocation.getArgument(1);
            responseListener.onFailure(new Exception("Failed to delete state"));
            return null;
        }).when(client).delete(any(DeleteRequest.class), any());

        flowFrameworkIndicesHandler.deleteFlowFrameworkSystemIndexDoc("1", listener);

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals("Failed to delete state", exceptionCaptor.getValue().getMessage());

        // test no index
        when(mockMetaData.hasIndex(WORKFLOW_STATE_INDEX)).thenReturn(false);
        flowFrameworkIndicesHandler.deleteFlowFrameworkSystemIndexDoc("1", listener);

        verify(listener, times(2)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "Failed to delete document 1 due to missing .plugins-flow-framework-state index",
            exceptionCaptor.getValue().getMessage()
        );
    }
}
