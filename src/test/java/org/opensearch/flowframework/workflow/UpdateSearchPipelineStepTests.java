/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.PIPELINE_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UpdateSearchPipelineStepTests extends OpenSearchTestCase {

    private WorkflowData inputData;
    private WorkflowData outpuData;
    private Client client;
    private AdminClient adminClient;
    private ClusterAdminClient clusterAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        String configurations =
            "{\"response_processors\":[{\"retrieval_augmented_generation\":{\"context_field_list\":[\"text\"],\"user_instructions\":\"Generate a concise and informative answer in less than 100 words for the given question\",\"description\":\"Demo pipeline Using OpenAI Connector\",\"tag\":\"openai_pipeline_demo\",\"model_id\":\"tbFoNI4BW58L8XKV4RF3\",\"system_prompt\":\"You are a helpful assistant\"}}]}";
        inputData = new WorkflowData(
            Map.ofEntries(Map.entry(CONFIGURATIONS, configurations), Map.entry(PIPELINE_ID, "pipelineId")),
            "test-id",
            "test-node-id"
        );

        // Set output data to returned pipelineId
        outpuData = new WorkflowData(Map.ofEntries(Map.entry(PIPELINE_ID, "pipelineId")), "test-id", "test-node-id");

        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
    }

    public void testUpdateSearchPipelineStep() throws InterruptedException, ExecutionException, IOException {

        UpdateSearchPipelineStep updateSearchPipelineStep = new UpdateSearchPipelineStep(client);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<AcknowledgedResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = updateSearchPipelineStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        assertFalse(future.isDone());

        // Mock put pipeline request execution and return true
        verify(clusterAdminClient, times(1)).putSearchPipeline(any(PutSearchPipelineRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new AcknowledgedResponse(true));

        assertTrue(future.isDone());
        assertEquals(outpuData.getContent(), future.get().getContent());

    }

    public void testUpdateSearchPipelineStepFailure() throws InterruptedException {

        UpdateSearchPipelineStep updateSearchPipelineStep = new UpdateSearchPipelineStep(client);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<AcknowledgedResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        PlainActionFuture<WorkflowData> future = updateSearchPipelineStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        assertFalse(future.isDone());

        // Mock put pipeline request execution and return false
        verify(clusterAdminClient, times(1)).putSearchPipeline(any(PutSearchPipelineRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onFailure(new Exception("Failed step update_search_pipeline"));

        assertTrue(future.isDone());

        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals("Failed step update_search_pipeline", exception.getCause().getMessage());
    }

    public void testMissingData() throws InterruptedException {
        UpdateSearchPipelineStep updateSearchPipelineStep = new UpdateSearchPipelineStep(client);
        // Data with missing input and output fields
        WorkflowData incorrectData = new WorkflowData(
            Map.ofEntries(
                Map.entry("id", PIPELINE_ID),
                Map.entry("description", "some description"),
                Map.entry("type", "text_embedding"),
                Map.entry(MODEL_ID, MODEL_ID)
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = updateSearchPipelineStep.execute(
            incorrectData.getNodeId(),
            incorrectData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );
        assertTrue(future.isDone());

        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals(
            "Missing required inputs [configurations, pipeline_id] in workflow [test-id] node [test-node-id]",
            exception.getCause().getMessage()
        );

    }

}
