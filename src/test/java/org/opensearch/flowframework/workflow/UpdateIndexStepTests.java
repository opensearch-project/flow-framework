/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateIndexStepTests extends OpenSearchTestCase {

    private Client client;
    private AdminClient adminClient;
    private IndicesAdminClient indicesAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        indicesAdminClient = mock(IndicesAdminClient.class);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
    }

    public void testUpdateIndexStepWithUpdatedSettings() throws ExecutionException, InterruptedException, IOException {

        UpdateIndexStep updateIndexStep = new UpdateIndexStep(client);

        String indexName = "test-index";

        // Create existing settings for default pipelines
        Settings.Builder builder = Settings.builder();
        builder.put("index.number_of_shards", 2);
        builder.put("index.number_of_replicas", 1);
        builder.put("index.knn", true);
        builder.put("index.default_pipeline", "ingest_pipeline_id");
        builder.put("index.search.default_pipeline", "search_pipeline_id");
        Map<String, Settings> indexToSettings = new HashMap<>();
        indexToSettings.put(indexName, builder.build());

        // Stub get index settings request/response
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> getSettingsResponseListener = invocation.getArgument(1);
            getSettingsResponseListener.onResponse(new GetSettingsResponse(indexToSettings, indexToSettings));
            return null;
        }).when(indicesAdminClient).getSettings(any(), any());

        // validate update settings request content
        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<UpdateSettingsRequest> updateSettingsRequestCaptor = ArgumentCaptor.forClass(UpdateSettingsRequest.class);

        // Configurations has updated search/ingest pipeline default values of _none
        String configurations =
            "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":2,\"number_of_replicas\":1,\"default_pipeline\":\"_none\",\"search\":{\"default_pipeline\":\"_none\"}}},\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}},\"aliases\":{\"sample-alias1\":{}}}";
        WorkflowData data = new WorkflowData(
            Map.ofEntries(Map.entry(INDEX_NAME, indexName), Map.entry(CONFIGURATIONS, configurations)),
            "test-id",
            "test-node-id"
        );
        PlainActionFuture future = updateIndexStep.execute(
            data.getNodeId(),
            data,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(indicesAdminClient, times(1)).getSettings(any(GetSettingsRequest.class), any());
        verify(indicesAdminClient, times(1)).updateSettings(updateSettingsRequestCaptor.capture(), any());

        Settings settingsToUpdate = updateSettingsRequestCaptor.getValue().settings();
        assertEquals(2, settingsToUpdate.size());
        assertEquals("_none", settingsToUpdate.get("index.default_pipeline"));
        assertEquals("_none", settingsToUpdate.get("index.search.default_pipeline"));
    }

    public void testMissingSettings() throws InterruptedException {
        UpdateIndexStep updateIndexStep = new UpdateIndexStep(client);

        String configurations = "{\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}},\"aliases\":{\"sample-alias1\":{}}}";

        // Data with empty configuration field
        WorkflowData incorrectData = new WorkflowData(
            Map.ofEntries(Map.entry(INDEX_NAME, "index-name"), Map.entry(CONFIGURATIONS, configurations)),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = updateIndexStep.execute(
            incorrectData.getNodeId(),
            incorrectData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals(
            "Failed to update index settings for index index-name, settings are not found in the index configuration",
            exception.getCause().getMessage()
        );
    }

    public void testEmptyConfiguration() throws InterruptedException {

        UpdateIndexStep updateIndexStep = new UpdateIndexStep(client);

        // Data with empty configuration field
        WorkflowData incorrectData = new WorkflowData(
            Map.ofEntries(Map.entry(INDEX_NAME, "index-name"), Map.entry(CONFIGURATIONS, "")),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = updateIndexStep.execute(
            incorrectData.getNodeId(),
            incorrectData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals(
            "Failed to update index settings for index index-name, index configuration is not given",
            exception.getCause().getMessage()
        );
    }

    public void testMissingInputs() throws InterruptedException {

        UpdateIndexStep updateIndexStep = new UpdateIndexStep(client);

        // Data with missing configuration field
        WorkflowData incorrectData = new WorkflowData(Map.ofEntries(Map.entry(INDEX_NAME, "index-name")), "test-id", "test-node-id");

        PlainActionFuture<WorkflowData> future = updateIndexStep.execute(
            incorrectData.getNodeId(),
            incorrectData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isDone());
        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals(
            "Missing required inputs [configurations] in workflow [test-id] node [test-node-id]",
            exception.getCause().getMessage()
        );

    }

    public void testNoSettingsChanged() throws InterruptedException {
        UpdateIndexStep updateIndexStep = new UpdateIndexStep(client);

        String indexName = "test-index";

        // Create existing settings for default pipelines
        Settings.Builder builder = Settings.builder();
        builder.put("index.number_of_shards", 2);
        builder.put("index.number_of_replicas", 1);
        builder.put("index.knn", true);
        builder.put("index.default_pipeline", "ingest_pipeline_id");
        builder.put("index.search.default_pipeline", "search_pipeline_id");
        Map<String, Settings> indexToSettings = new HashMap<>();
        indexToSettings.put(indexName, builder.build());

        // Stub get index settings request/response
        doAnswer(invocation -> {
            ActionListener<GetSettingsResponse> getSettingsResponseListener = invocation.getArgument(1);
            getSettingsResponseListener.onResponse(new GetSettingsResponse(indexToSettings, indexToSettings));
            return null;
        }).when(indicesAdminClient).getSettings(any(), any());

        // validate update settings request content
        @SuppressWarnings({ "unchecked" })
        ArgumentCaptor<UpdateSettingsRequest> updateSettingsRequestCaptor = ArgumentCaptor.forClass(UpdateSettingsRequest.class);

        // Configurations have no change
        String configurations =
            "{\"settings\":{\"index\":{\"knn\":true,\"number_of_shards\":2,\"number_of_replicas\":1,\"default_pipeline\":\"ingest_pipeline_id\",\"search\":{\"default_pipeline\":\"search_pipeline_id\"}}},\"mappings\":{\"properties\":{\"age\":{\"type\":\"integer\"}}},\"aliases\":{\"sample-alias1\":{}}}";
        WorkflowData data = new WorkflowData(
            Map.ofEntries(Map.entry(INDEX_NAME, indexName), Map.entry(CONFIGURATIONS, configurations)),
            "test-id",
            "test-node-id"
        );
        PlainActionFuture future = updateIndexStep.execute(
            data.getNodeId(),
            data,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals(
            "Failed to update index settings for index test-index, no settings have been updated",
            exception.getCause().getMessage()
        );
    }

}
