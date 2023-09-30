/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow.RegisterModel;

import org.opensearch.client.node.NodeClient;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.*;

public class RegisterModelTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock(answer = RETURNS_DEEP_STUBS)
    NodeClient client;

    MachineLearningNodeClient machineLearningNodeClient;

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

        machineLearningNodeClient = mock(MachineLearningNodeClient.class);

    }

    public void testRegisterModel() {

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

        RegisterModelStep registerModelStep = new RegisterModelStep(client);

        ArgumentCaptor<MLRegisterModelResponse> argumentCaptor = ArgumentCaptor.forClass(MLRegisterModelResponse.class);
        CompletableFuture<WorkflowData> future = registerModelStep.execute(List.of(inputData));

        verify(machineLearningNodeClient, times(1)).register(mlInput);
        assertEquals("1", (argumentCaptor.getValue()).getTaskId());

        assertTrue(future.isDone());
    }

}
