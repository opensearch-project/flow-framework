/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.connector.ConnectorAction;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorInput;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CreateConnectorStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        Map<String, String> params = Map.ofEntries(Map.entry("endpoint", "endpoint"), Map.entry("temp", "7"));
        Map<String, String> credentials = Map.ofEntries(Map.entry("key1", "value1"), Map.entry("key2", "value2"));
        Map<?, ?>[] actions = new Map<?, ?>[] {
            Map.ofEntries(
                Map.entry(ConnectorAction.ACTION_TYPE_FIELD, ConnectorAction.ActionType.PREDICT.name()),
                Map.entry(ConnectorAction.METHOD_FIELD, "post"),
                Map.entry(ConnectorAction.URL_FIELD, "foo.test"),
                Map.entry(
                    ConnectorAction.REQUEST_BODY_FIELD,
                    "{ \"model\": \"${parameters.model}\", \"messages\": ${parameters.messages} }"
                )
            ) };

        MockitoAnnotations.openMocks(this);

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry(CommonValue.NAME_FIELD, "test"),
                Map.entry(CommonValue.DESCRIPTION_FIELD, "description"),
                Map.entry(CommonValue.VERSION_FIELD, "1"),
                Map.entry(CommonValue.PROTOCOL_FIELD, "test"),
                Map.entry(CommonValue.PARAMETERS_FIELD, params),
                Map.entry(CommonValue.CREDENTIAL_FIELD, credentials),
                Map.entry(CommonValue.ACTIONS_FIELD, actions)
            ),
            "test-id",
            "test-node-id"
        );
    }

    public void testCreateConnector() throws IOException, ExecutionException, InterruptedException {

        String connectorId = "connect";
        CreateConnectorStep createConnectorStep = new CreateConnectorStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        doAnswer(invocation -> {
            ActionListener<MLCreateConnectorResponse> actionListener = invocation.getArgument(1);
            MLCreateConnectorResponse output = new MLCreateConnectorResponse(connectorId);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).createConnector(any(MLCreateConnectorInput.class), any());

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateResourceInStateIndex(anyString(), anyString(), anyString(), anyString(), any());

        PlainActionFuture<WorkflowData> future = createConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(machineLearningNodeClient).createConnector(any(MLCreateConnectorInput.class), any());
        assertTrue(future.isDone());
        assertEquals(connectorId, future.get().getContent().get(CONNECTOR_ID));

    }

    public void testCreateConnectorFailure() throws IOException {
        CreateConnectorStep createConnectorStep = new CreateConnectorStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        doAnswer(invocation -> {
            ActionListener<MLCreateConnectorResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to create connector", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).createConnector(any(MLCreateConnectorInput.class), any());

        PlainActionFuture<WorkflowData> future = createConnectorStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(machineLearningNodeClient).createConnector(any(MLCreateConnectorInput.class), any());

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to create connector", ex.getCause().getMessage());
    }

}
