/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.FrameworkType;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.TextEmbeddingModelConfigBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput.MLRegisterModelInputBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.ALL_CONFIG;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.EMBEDDING_DIMENSION;
import static org.opensearch.flowframework.common.CommonValue.FRAMEWORK_TYPE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_CONTENT_HASH_VALUE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_FORMAT;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.CommonValue.MODEL_TYPE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.URL;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;

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
        super(settings, clusterService, mlClient, flowFrameworkIndicesHandler);
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
                retryableGetMlTask(
                    currentNodeInputs.getWorkflowId(),
                    currentNodeId,
                    registerLocalModelFuture,
                    taskId,
                    0,
                    "Local model registration"
                );
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
            MODEL_TYPE,
            EMBEDDING_DIMENSION,
            FRAMEWORK_TYPE,
            MODEL_CONTENT_HASH_VALUE,
            URL
        );
        Set<String> optionalKeys = Set.of(DESCRIPTION_FIELD, MODEL_GROUP_ID, ALL_CONFIG);

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
}
