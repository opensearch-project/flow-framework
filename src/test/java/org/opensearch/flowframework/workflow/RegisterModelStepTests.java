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

import org.mockito.*;

import static org.mockito.Mockito.*;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RegisterModelStepTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private NodeClient nodeClient;

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

        MLRegisterModelInput mlInput = MLRegisterModelInput.builder()
            .functionName(FunctionName.from("REMOTE"))
            .modelName("testModelName")
            .description("description")
            .connectorId("abcdefgh")
            .build();

        RegisterModelStep registerModelStep = new RegisterModelStep(nodeClient);

        ArgumentCaptor<ActionListener<MLRegisterModelResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        CompletableFuture<WorkflowData> future = registerModelStep.execute(List.of(inputData));

        /*try (MockedStatic<MLClient> mlClientMockedStatic = Mockito.mockStatic(MLClient.class)) {
            mlClientMockedStatic
                    .when(() -> MLClient.createMLClient(any(NodeClient.class)))
                    .thenReturn(machineLearningNodeClient);

        }*/
        // when(spy(MLClient.createMLClient(nodeClient))).thenReturn(machineLearningNodeClient);
        verify(machineLearningNodeClient, times(1)).register(any(MLRegisterModelInput.class), actionListenerCaptor.capture());
        actionListenerCaptor.getValue().onResponse(new MLRegisterModelResponse("xyz", MLTaskState.COMPLETED.name(), "abc"));

        assertTrue(future.isDone() && !future.isCompletedExceptionally());

        Map<String, Object> outputData = Map.of("index-name", "demo");

        assertTrue(future.isDone() && future.isCompletedExceptionally());

        assertEquals(outputData, future.get().getContent());

    }

}
