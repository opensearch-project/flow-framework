/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import static org.opensearch.flowframework.common.CommonValue.ALL_CONFIG;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.EMBEDDING_DIMENSION;
import static org.opensearch.flowframework.common.CommonValue.FRAMEWORK_TYPE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_CONTENT_HASH_VALUE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_FORMAT;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.CommonValue.MODEL_TYPE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.URL;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.FrameworkType;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.TextEmbeddingModelConfigBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput.MLRegisterModelInputBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;

/**
 * Step to register a local model
 */
public class RegisterLocalModelStep extends AbstractRetryableWorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterLocalModelStep.class);

    private final MachineLearningNodeClient mlClient;

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    static final String NAME = WorkflowResources.REGISTER_LOCAL_MODEL.getWorkflowStep();

    /**
     * Instantiate this class
     * @param settings The OpenSearch settings
     * @param clusterService The cluster service
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public RegisterLocalModelStep(
        Settings settings,
        ClusterService clusterService,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler
    ) {
        super(settings, clusterService);
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {

        CompletableFuture<WorkflowData> registerLocalModelFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelResponse mlRegisterModelResponse) {
                logger.info("Local Model registration task creation successful");

                String taskId = mlRegisterModelResponse.getTaskId();

                // Attempt to retrieve the model ID
                retryableGetMlTask(currentNodeInputs.getWorkflowId(), currentNodeId, registerLocalModelFuture, taskId, 0);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register local model");
                registerLocalModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(
            NAME_FIELD,
            VERSION_FIELD,
            MODEL_FORMAT,
            MODEL_GROUP_ID,
            MODEL_TYPE,
            EMBEDDING_DIMENSION,
            FRAMEWORK_TYPE,
            MODEL_CONTENT_HASH_VALUE,
            URL
        );
        Set<String> optionalKeys = Set.of(DESCRIPTION_FIELD, ALL_CONFIG);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs
            );

            String modelName = (String) inputs.get(NAME_FIELD);
            String modelVersion = (String) inputs.get(VERSION_FIELD);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            MLModelFormat modelFormat = MLModelFormat.from((String) inputs.get(MODEL_FORMAT));
            String modelGroupId = (String) inputs.get(MODEL_GROUP_ID);
            String modelContentHashValue = (String) inputs.get(MODEL_CONTENT_HASH_VALUE);
            String modelType = (String) inputs.get(MODEL_TYPE);
            String embeddingDimension = (String) inputs.get(EMBEDDING_DIMENSION);
            FrameworkType frameworkType = FrameworkType.from((String) inputs.get(FRAMEWORK_TYPE));
            String allConfig = (String) inputs.get(ALL_CONFIG);
            String url = (String) inputs.get(URL);

            // Create Model configuration
            TextEmbeddingModelConfigBuilder modelConfigBuilder = TextEmbeddingModelConfig.builder()
                .modelType(modelType)
                .embeddingDimension(Integer.valueOf(embeddingDimension))
                .frameworkType(frameworkType);
            if (allConfig != null) {
                modelConfigBuilder.allConfig(allConfig);
            }
            MLModelConfig modelConfig = modelConfigBuilder.build();

            // Create register local model input
            MLRegisterModelInputBuilder mlInputBuilder = MLRegisterModelInput.builder()
                .modelName(modelName)
                .version(modelVersion)
                .modelFormat(modelFormat)
                .modelGroupId(modelGroupId)
                .hashValue(modelContentHashValue)
                .modelConfig(modelConfig)
                .url(url);
            if (description != null) {
                mlInputBuilder.description(description);
            }

            MLRegisterModelInput mlInput = mlInputBuilder.build();

            mlClient.register(mlInput, actionListener);
        } catch (FlowFrameworkException e) {
            registerLocalModelFuture.completeExceptionally(e);
        }
        return registerLocalModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Retryable get ml task
     * @param workflowId the workflow id
     * @param nodeId the workflow node id
     * @param registerLocalModelFuture the workflow step future
     * @param taskId the ml task id
     * @param retries the current number of request retries
     */
    void retryableGetMlTask(
        String workflowId,
        String nodeId,
        CompletableFuture<WorkflowData> registerLocalModelFuture,
        String taskId,
        int retries
    ) {
        mlClient.getTask(taskId, ActionListener.wrap(response -> {
            MLTaskState currentState = response.getState();
            if (currentState != MLTaskState.COMPLETED) {
                if (Stream.of(MLTaskState.FAILED, MLTaskState.COMPLETED_WITH_ERROR).anyMatch(x -> x == currentState)) {
                    // Model registration failed or completed with errors
                    String errorMessage = "Local model registration failed with error : " + response.getError();
                    logger.error(errorMessage);
                    registerLocalModelFuture.completeExceptionally(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                } else {
                    // Task still in progress, attempt retry
                    throw new IllegalStateException("Local model registration is not yet completed");
                }
            } else {
                try {
                    logger.info("Local Model registration successful");
                    String resourceName = WorkflowResources.getResourceByWorkflowStep(getName());
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        workflowId,
                        nodeId,
                        getName(),
                        response.getTaskId(),
                        ActionListener.wrap(updateResponse -> {
                            logger.info("successfully updated resources created in state index: {}", updateResponse.getIndex());
                            registerLocalModelFuture.complete(
                                new WorkflowData(
                                    Map.ofEntries(
                                        Map.entry(resourceName, response.getModelId()),
                                        Map.entry(REGISTER_MODEL_STATUS, response.getState().name())
                                    ),
                                    workflowId,
                                    nodeId
                                )
                            );
                        }, exception -> {
                            logger.error("Failed to update new created resource", exception);
                            registerLocalModelFuture.completeExceptionally(
                                new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                            );
                        })
                    );

                } catch (Exception e) {
                    logger.error("Failed to parse and update new created resource", e);
                    registerLocalModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }
            }
        }, exception -> {
            if (retries < maxRetry) {
                // Sleep thread prior to retrying request
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    FutureUtils.cancel(registerLocalModelFuture);
                }
                final int retryAdd = retries + 1;
                retryableGetMlTask(workflowId, nodeId, registerLocalModelFuture, taskId, retryAdd);
            } else {
                logger.error("Failed to retrieve local model registration task, maximum retries exceeded");
                registerLocalModelFuture.completeExceptionally(
                    new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                );
            }
        }));
    }
}
