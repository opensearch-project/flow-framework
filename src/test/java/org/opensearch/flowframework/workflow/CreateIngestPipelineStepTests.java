/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("deprecation")
public class CreateIngestPipelineStepTests extends OpenSearchTestCase {

    private WorkflowData inputData;
    private WorkflowData outpuData;
    private Client client;
    private AdminClient adminClient;
    private ClusterAdminClient clusterAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("id", "pipelineId"),
                Map.entry("description", "some description"),
                Map.entry("type", "text_embedding"),
                Map.entry("model_id", "model_id"),
                Map.entry("input_field_name", "inputField"),
                Map.entry("output_field_name", "outputField")
            )
        );

        // Set output data to returned pipelineId
        outpuData = new WorkflowData(Map.ofEntries(Map.entry("pipelineId", "pipelineId")));

        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        clusterAdminClient = mock(ClusterAdminClient.class);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
    }

    public void testCreateIngestPipelineStep() throws InterruptedException, ExecutionException {

        CreateIngestPipelineStep createIngestPipelineStep = new CreateIngestPipelineStep(client);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<AcknowledgedResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = createIngestPipelineStep.execute(List.of(inputData));

        assertFalse(future.isDone());

        // Mock put pipeline request execution and return true
        verify(clusterAdminClient, times(1)).putPipeline(any(PutPipelineRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new AcknowledgedResponse(true));

        assertTrue(future.isDone() && !future.isCompletedExceptionally());
        assertEquals(outpuData.getContent(), future.get().getContent());
    }

    public void testCreateIngestPipelineStepFailure() throws InterruptedException {

        CreateIngestPipelineStep createIngestPipelineStep = new CreateIngestPipelineStep(client);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<AcknowledgedResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = createIngestPipelineStep.execute(List.of(inputData));

        assertFalse(future.isDone());

        // Mock put pipeline request execution and return false
        verify(clusterAdminClient, times(1)).putPipeline(any(PutPipelineRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onFailure(new Exception("Failed to create ingest pipeline"));

        assertTrue(future.isDone() && future.isCompletedExceptionally());

        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals("Failed to create ingest pipeline", exception.getCause().getMessage());
    }

    public void testMissingData() throws InterruptedException {
        CreateIngestPipelineStep createIngestPipelineStep = new CreateIngestPipelineStep(client);

        // Data with missing input and output fields
        WorkflowData incorrectData = new WorkflowData(
            Map.ofEntries(
                Map.entry("id", "pipelineId"),
                Map.entry("description", "some description"),
                Map.entry("type", "text_embedding"),
                Map.entry("model_id", "model_id")
            )
        );

        CompletableFuture<WorkflowData> future = createIngestPipelineStep.execute(List.of(incorrectData));
        assertTrue(future.isDone() && future.isCompletedExceptionally());

        ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get());
        assertTrue(exception.getCause() instanceof Exception);
        assertEquals("Failed to create ingest pipeline, required inputs not found", exception.getCause().getMessage());
    }

}
