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

import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.TASK_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RegisterLocalModelStepTests extends OpenSearchTestCase {

    private RegisterLocalModelStep registerLocalModelStep;
    private WorkflowData workflowData;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        MockitoAnnotations.openMocks(this);
        this.registerLocalModelStep = new RegisterLocalModelStep(machineLearningNodeClient);

        MLModelConfig config = TextEmbeddingModelConfig.builder()
            .modelType("testModelType")
            .allConfig("{\"field1\":\"value1\",\"field2\":\"value2\"}")
            .frameworkType(TextEmbeddingModelConfig.FrameworkType.SENTENCE_TRANSFORMERS)
            .embeddingDimension(100)
            .build();

        this.workflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "xyz"),
                Map.entry("version", "1.0.0"),
                Map.entry("description", "description"),
                Map.entry("model_format", "TORCH_SCRIPT"),
                Map.entry("model_group_id", "abcdefg"),
                Map.entry("model_content_hash_value", "aiwoeifjoaijeofiwe"),
                Map.entry("model_type", "bert"),
                Map.entry("embedding_dimension", "384"),
                Map.entry("framework_type", "sentence_transformers"),
                Map.entry("url", "something.com")
            ),
            "test-id"
        );

    }

    public void testRegisterLocalModelSuccess() throws Exception {

        String taskId = "abcd";
        String status = MLTaskState.CREATED.name();

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelResponse output = new MLRegisterModelResponse(taskId, status, null);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).register(any(MLRegisterModelInput.class), any());

        CompletableFuture<WorkflowData> future = registerLocalModelStep.execute(List.of(workflowData));
        verify(machineLearningNodeClient).register(any(MLRegisterModelInput.class), any());

        assertTrue(future.isDone());
        assertTrue(!future.isCompletedExceptionally());
        assertEquals(taskId, future.get().getContent().get(TASK_ID));
        assertEquals(status, future.get().getContent().get(REGISTER_MODEL_STATUS));

    }

    public void testRegisterLocalModelFailure() {

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new IllegalArgumentException("test"));
            return null;
        }).when(machineLearningNodeClient).register(any(MLRegisterModelInput.class), any());

        CompletableFuture<WorkflowData> future = this.registerLocalModelStep.execute(List.of(workflowData));
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = expectThrows(ExecutionException.class, () -> future.get().getClass());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("test", ex.getCause().getMessage());
    }

    public void testMissingInputs() {
        CompletableFuture<WorkflowData> future = registerLocalModelStep.execute(List.of(WorkflowData.EMPTY));
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Required fields are not provided", ex.getCause().getMessage());
    }

}
