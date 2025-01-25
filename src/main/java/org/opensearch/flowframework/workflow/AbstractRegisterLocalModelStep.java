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
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.FrameworkType;
import org.opensearch.ml.common.model.TextEmbeddingModelConfig.TextEmbeddingModelConfigBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput.MLRegisterModelInputBuilder;
import org.opensearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.ALL_CONFIG;
import static org.opensearch.flowframework.common.CommonValue.DEPLOY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.EMBEDDING_DIMENSION;
import static org.opensearch.flowframework.common.CommonValue.FRAMEWORK_TYPE;
import static org.opensearch.flowframework.common.CommonValue.FUNCTION_NAME;
import static org.opensearch.flowframework.common.CommonValue.INTERFACE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MODEL_CONTENT_HASH_VALUE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_FORMAT;
import static org.opensearch.flowframework.common.CommonValue.MODEL_TYPE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.URL;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Abstract local model registration step
 */
public abstract class AbstractRegisterLocalModelStep extends AbstractRetryableWorkflowStep {

    private static final Logger logger = LogManager.getLogger(AbstractRegisterLocalModelStep.class);
    private final MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /**
     * Instantiate this class
     * @param threadPool The OpenSearch thread pool
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     * @param flowFrameworkSettings settings of flow framework
     */
    protected AbstractRegisterLocalModelStep(
        ThreadPool threadPool,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        super(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings);
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params,
        String tenantId
    ) {

        PlainActionFuture<WorkflowData> registerLocalModelFuture = PlainActionFuture.newFuture();

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                getRequiredKeys(),
                getOptionalKeys(),
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            // Extract common fields of OS provided text-embedding, sparse encoding and custom models
            String modelName = (String) inputs.get(NAME_FIELD);
            String modelVersion = (String) inputs.get(VERSION_FIELD);
            String modelFormat = (String) inputs.get(MODEL_FORMAT);

            // Extract non-common fields
            String functionName = (String) inputs.get(FUNCTION_NAME);
            String modelContentHashValue = (String) inputs.get(MODEL_CONTENT_HASH_VALUE);
            String url = (String) inputs.get(URL);
            String modelType = (String) inputs.get(MODEL_TYPE);
            String embeddingDimension = (String) inputs.get(EMBEDDING_DIMENSION);
            String frameworkType = (String) inputs.get(FRAMEWORK_TYPE);

            // Extract optional fields
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            String modelGroupId = (String) inputs.get(MODEL_GROUP_ID);
            String allConfig = (String) inputs.get(ALL_CONFIG);
            String modelInterface = (String) inputs.get(INTERFACE_FIELD);
            final Boolean deploy = ParseUtils.parseIfExists(inputs, DEPLOY_FIELD, Boolean.class);

            // Build register model input
            MLRegisterModelInputBuilder mlInputBuilder = MLRegisterModelInput.builder()
                .modelName(modelName)
                .version(modelVersion)
                .modelFormat(MLModelFormat.from(modelFormat));

            if (functionName != null) {
                mlInputBuilder.functionName(FunctionName.from(functionName));
            }
            if (modelContentHashValue != null) {
                mlInputBuilder.hashValue(modelContentHashValue);
            }
            if (url != null) {
                mlInputBuilder.url(url);
            }
            if (Stream.of(modelType, embeddingDimension, frameworkType).allMatch(x -> x != null)) {
                TextEmbeddingModelConfigBuilder mlConfigBuilder = TextEmbeddingModelConfig.builder()
                    .modelType(modelType)
                    .embeddingDimension(Integer.valueOf(embeddingDimension))
                    .frameworkType(FrameworkType.from(frameworkType));
                if (allConfig != null) {
                    mlConfigBuilder.allConfig(allConfig);
                }
                mlInputBuilder.modelConfig(mlConfigBuilder.build());
            }
            if (description != null) {
                mlInputBuilder.description(description);
            }
            if (modelGroupId != null) {
                mlInputBuilder.modelGroupId(modelGroupId);
            }
            if (modelInterface != null) {
                try {
                    // Convert model interface string to map
                    BytesReference modelInterfaceBytes = new BytesArray(modelInterface.getBytes(StandardCharsets.UTF_8));
                    Map<String, Object> modelInterfaceAsMap = XContentHelper.convertToMap(
                        modelInterfaceBytes,
                        false,
                        MediaTypeRegistry.JSON
                    ).v2();

                    // Convert to string to string map
                    Map<String, String> parameters = ParseUtils.convertStringToObjectMapToStringToStringMap(modelInterfaceAsMap);
                    mlInputBuilder.modelInterface(parameters);

                } catch (Exception ex) {
                    String errorMessage = "Failed to create model interface";
                    logger.error(errorMessage, ex);
                    registerLocalModelFuture.onFailure(new WorkflowStepException(errorMessage, RestStatus.BAD_REQUEST));
                }

            }
            if (deploy != null) {
                mlInputBuilder.deployModel(deploy);
            }

            MLRegisterModelInput mlInput = mlInputBuilder.build();

            mlClient.register(mlInput, ActionListener.wrap(response -> {
                logger.info("Local Model registration task creation successful");

                String taskId = response.getTaskId();

                // Attempt to retrieve the model ID
                retryableGetMlTask(
                    currentNodeInputs,
                    currentNodeId,
                    registerLocalModelFuture,
                    taskId,
                    "Local model registration",
                    tenantId,
                    ActionListener.wrap(mlTaskWorkflowData -> {
                        // Registered Model Resource has been updated
                        String resourceName = getResourceByWorkflowStep(getName());
                        if (Boolean.TRUE.equals(deploy)) {
                            String id = (String) mlTaskWorkflowData.getContent().get(resourceName);
                            ActionListener<WorkflowData> deployUpdateListener = ActionListener.wrap(
                                deployUpdateResponse -> registerLocalModelFuture.onResponse(mlTaskWorkflowData),
                                deployUpdateException -> {
                                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                        "Failed to update simulated deploy step resource {}",
                                        id
                                    ).getFormattedMessage();
                                    logger.error(errorMessage, deployUpdateException);
                                    registerLocalModelFuture.onFailure(
                                        new FlowFrameworkException(errorMessage, ExceptionsHelper.status(deployUpdateException))
                                    );
                                }
                            );
                            // Simulate Model deployment step and update resources created
                            flowFrameworkIndicesHandler.addResourceToStateIndex(
                                currentNodeInputs,
                                currentNodeId,
                                DeployModelStep.NAME,
                                id,
                                tenantId,
                                deployUpdateListener
                            );
                        } else {
                            registerLocalModelFuture.onResponse(mlTaskWorkflowData);
                        }
                    }, registerLocalModelFuture::onFailure)
                );
            }, exception -> {
                Exception e = getSafeException(exception);
                String errorMessage = (e == null
                    ? ParameterizedMessageFactory.INSTANCE.newMessage("Failed to register local model in step {}", currentNodeId)
                        .getFormattedMessage()
                    : e.getMessage());
                logger.error(errorMessage, e);
                registerLocalModelFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }));
        } catch (IllegalArgumentException iae) {
            registerLocalModelFuture.onFailure(new WorkflowStepException(iae.getMessage(), RestStatus.BAD_REQUEST));
        } catch (FlowFrameworkException e) {
            registerLocalModelFuture.onFailure(e);
        }
        return registerLocalModelFuture;
    }

    /**
     * Returns the required keys of the local model step
     * @return the set of required keys
     */
    protected abstract Set<String> getRequiredKeys();

    /**
     * Returns the optional keys of the local model step
     * @return the set of optional keys
     */
    protected abstract Set<String> getOptionalKeys();

}
