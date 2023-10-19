/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorInput;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

public class CreateConnectorStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    ActionListener<MLCreateConnectorResponse> registerModelActionListener;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Map<String, String> params = Map.ofEntries(Map.entry("endpoint", "endpoint"), Map.entry("temp", "7"));
        Map<String, String> credentials = Map.ofEntries(Map.entry("key1", "value1"), Map.entry("key2", "value2"));

        MockitoAnnotations.openMocks(this);

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("version", "1"),
                Map.entry("protocol", "test"),
                Map.entry("params", params),
                Map.entry("credentials", credentials),
                Map.entry("actions", List.of("actions"))
            )
        );

    }

    public void testCreateConnector() throws IOException {

        String connectorId = "connect";
        CreateConnectorStep createConnectorStep = new CreateConnectorStep(machineLearningNodeClient);

        ArgumentCaptor<ActionListener<MLCreateConnectorResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLCreateConnectorResponse> actionListener = invocation.getArgument(1);
            MLCreateConnectorResponse output = new MLCreateConnectorResponse(connectorId);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).createConnector(any(MLCreateConnectorInput.class), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = createConnectorStep.execute(List.of(inputData));

        verify(machineLearningNodeClient).createConnector(any(MLCreateConnectorInput.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());

    }

}
