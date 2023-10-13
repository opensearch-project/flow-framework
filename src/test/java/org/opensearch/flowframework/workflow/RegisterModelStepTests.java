/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RegisterModelStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private NodeClient nodeClient;

    @Mock
    ActionListener<MLRegisterModelResponse> registerModelActionListener;

    @Mock
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

        MockitoAnnotations.openMocks(this);

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("function_name", "remote"),
                Map.entry("name", "xyz"),
                Map.entry("description", "description"),
                Map.entry("connector_id", "abcdefg")
            )
        );

        nodeClient = new NoOpNodeClient("xyz");
    }

    public void testRegisterModel() throws ExecutionException, InterruptedException {

        String taskId = "abcd";
        String modelId = "efgh";
        String status = MLTaskState.CREATED.name();
        MLRegisterModelInput mlInput = MLRegisterModelInput.builder()
            .functionName(FunctionName.from("REMOTE"))
            .modelName("testModelName")
            .description("description")
            .connectorId("abcdefgh")
            .build();

        RegisterModelStep registerModelStep = new RegisterModelStep(nodeClient);

        ArgumentCaptor<ActionListener<MLRegisterModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(2);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, status, modelId);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).register(any(MLRegisterModelInput.class), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = registerModelStep.execute(List.of(inputData));

        // TODO: Find a way to verify the below
        // verify(machineLearningNodeClient).register(any(MLRegisterModelInput.class), actionListenerCaptor.capture());

        assertTrue(future.isCompletedExceptionally());

    }

}
