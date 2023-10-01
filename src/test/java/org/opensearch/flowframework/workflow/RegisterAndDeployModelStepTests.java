/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.client.MLClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.*;

import static org.mockito.Mockito.*;

public class RegisterAndDeployModelStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private NodeClient nodeClient;

    private MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MLModelConfig config = TextEmbeddingModelConfig.builder()
            .modelType("testModelType")
            .allConfig("{\"field1\":\"value1\",\"field2\":\"value2\"}")
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.SENTENCE_TRANSFORMERS)
            .embeddingDimension(100)
            .build();

        inputData = new WorkflowData(
            Map.of(
                "function_name",
                FunctionName.KMEANS,
                "model_name",
                "bedrock",
                "model_version",
                "1.0.0",
                "model_group_id",
                "1.0",
                "model_format",
                MLModelFormat.TORCH_SCRIPT,
                "model_config",
                config,
                "description",
                "description",
                "connector_id",
                "abcdefgh"
            )
        );

        nodeClient = mock(NodeClient.class);

    }

    public void testRegisterModel() throws ExecutionException, InterruptedException {

        FunctionName functionName = FunctionName.KMEANS;

        MLModelConfig config = TextEmbeddingModelConfig.builder()
            .modelType("testModelType")
            .allConfig("{\"field1\":\"value1\",\"field2\":\"value2\"}")
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.SENTENCE_TRANSFORMERS)
            .embeddingDimension(100)
            .build();

        MLRegisterModelInput mlInput = MLRegisterModelInput.builder()
            .functionName(functionName)
            .modelName("testModelName")
            .version("testModelVersion")
            .modelGroupId("modelGroupId")
            .url("url")
            .modelFormat(MLModelFormat.ONNX)
            .modelConfig(config)
            .description("description")
            .connectorId("abcdefgh")
            .build();

        RegisterAndDeployModelStep registerModelStep = new RegisterAndDeployModelStep(nodeClient);

        ArgumentCaptor<ActionListener<MLRegisterModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = registerModelStep.execute(List.of(inputData));

        assertFalse(future.isDone());

        /*try (MockedStatic<MLClient> mlClientMockedStatic = Mockito.mockStatic(MLClient.class)) {
            mlClientMockedStatic
                    .when(() -> MLClient.createMLClient(any(NodeClient.class)))
                    .thenReturn(machineLearningNodeClient);

        }*/
        when(spy(MLClient.createMLClient(nodeClient))).thenReturn(machineLearningNodeClient);
        verify(machineLearningNodeClient, times(1)).register(any(MLRegisterModelInput.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new MLRegisterModelResponse("xyz", MLTaskState.COMPLETED.name(), "abc"));

        assertTrue(future.isDone() && !future.isCompletedExceptionally());

        Map<String, Object> outputData = Map.of("index-name", "demo");

        assertTrue(future.isDone() && future.isCompletedExceptionally());

        assertEquals(outputData, future.get().getContent());

    }

}
