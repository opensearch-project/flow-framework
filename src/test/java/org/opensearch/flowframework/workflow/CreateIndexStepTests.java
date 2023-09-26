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
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CreateIndexStepTests extends OpenSearchTestCase {

    private WorkflowData inputData = WorkflowData.EMPTY;

    private Client client;

    private AdminClient adminClient;

    private IndicesAdminClient indicesAdminClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        inputData = new WorkflowData(Map.ofEntries(Map.entry("index-name", "demo"), Map.entry("type", "knn")));
        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        indicesAdminClient = mock(IndicesAdminClient.class);

        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);

    }

    public void testCreateIndexStep() throws ExecutionException, InterruptedException, IOException {

        CreateIndexStep createIndexStep = new CreateIndexStep(client);

        ArgumentCaptor<ActionListener> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = createIndexStep.execute(List.of(inputData));
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new CreateIndexResponse(true, true, "demo"));

        assertTrue(future.isDone() && !future.isCompletedExceptionally());

        Map<String, Object> outputData = Map.of("index-name", "demo");
        assertEquals(outputData, future.get().getContent());

    }

    public void testCreateIndexStepFailure() throws ExecutionException, InterruptedException {

        CreateIndexStep createIndexStep = new CreateIndexStep(client);

        ArgumentCaptor<ActionListener> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = createIndexStep.execute(List.of(inputData));
        assertFalse(future.isDone());
        verify(indicesAdminClient, times(1)).create(any(CreateIndexRequest.class), actionListenerCaptor.capture());

        actionListenerCaptor.getValue().onFailure(new Exception("Failed to create an index"));

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof Exception);
        assertEquals("Failed to create an index", ex.getCause().getMessage());
    }
}
