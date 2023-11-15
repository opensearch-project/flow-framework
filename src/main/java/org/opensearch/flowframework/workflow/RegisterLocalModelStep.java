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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.FrameworkType;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.TextEmbeddingModelConfigBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput.MLRegisterModelInputBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

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
import static org.opensearch.flowframework.common.CommonValue.TASK_ID;
import static org.opensearch.flowframework.common.CommonValue.URL;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;

/**
 * Step to register a local model
 */
public class RegisterLocalModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterLocalModelStep.class);

    private MachineLearningNodeClient mlClient;

    static final String NAME = "register_local_model";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public RegisterLocalModelStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> registerLocalModelFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelResponse mlRegisterModelResponse) {
                logger.info("Local Model registration task creation successful");
                registerLocalModelFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(
                            Map.entry(TASK_ID, mlRegisterModelResponse.getTaskId()),
                            Map.entry(REGISTER_MODEL_STATUS, mlRegisterModelResponse.getStatus())
                        ),
                        data.get(0).getWorkflowId()
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register local model");
                registerLocalModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        String modelName = null;
        String modelVersion = null;
        String description = null;
        MLModelFormat modelFormat = null;
        String modelGroupId = null;
        String modelContentHashValue = null;
        String modelType = null;
        String embeddingDimension = null;
        FrameworkType frameworkType = null;
        String allConfig = null;
        String url = null;

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        modelName = (String) content.get(NAME_FIELD);
                        break;
                    case VERSION_FIELD:
                        modelVersion = (String) content.get(VERSION_FIELD);
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) content.get(DESCRIPTION_FIELD);
                        break;
                    case MODEL_FORMAT:
                        modelFormat = MLModelFormat.from((String) content.get(MODEL_FORMAT));
                        break;
                    case MODEL_GROUP_ID:
                        modelGroupId = (String) content.get(MODEL_GROUP_ID);
                        break;
                    case MODEL_TYPE:
                        modelType = (String) content.get(MODEL_TYPE);
                        break;
                    case EMBEDDING_DIMENSION:
                        embeddingDimension = (String) content.get(EMBEDDING_DIMENSION);
                        break;
                    case FRAMEWORK_TYPE:
                        frameworkType = FrameworkType.from((String) content.get(FRAMEWORK_TYPE));
                        break;
                    case ALL_CONFIG:
                        allConfig = (String) content.get(ALL_CONFIG);
                        break;
                    case MODEL_CONTENT_HASH_VALUE:
                        modelContentHashValue = (String) content.get(MODEL_CONTENT_HASH_VALUE);
                        break;
                    case URL:
                        url = (String) content.get(URL);
                        break;
                    default:
                        break;

                }
            }
        }

        if (Stream.of(
            modelName,
            modelVersion,
            modelFormat,
            modelGroupId,
            modelType,
            embeddingDimension,
            frameworkType,
            modelContentHashValue,
            url
        ).allMatch(x -> x != null)) {

            // Create Model configudation
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
        } else {
            registerLocalModelFuture.completeExceptionally(
                new FlowFrameworkException("Required fields are not provided", RestStatus.BAD_REQUEST)
            );
        }

        return registerLocalModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
