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
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.client.MLClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class RegisterAndDeployModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterAndDeployModelStep.class);

    private Client client;
    private ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;

    static final String NAME = "register_model_step";

    private static final String FUNCTION_NAME = "function_name";
    private static final String MODEL_NAME = "model_name";
    private static final String MODEL_VERSION = "model_version";
    private static final String MODEL_GROUP_ID = "model_group_id";
    private static final String DESCRIPTION = "description";
    private static final String CONNECTOR_ID = "connector_id";
    private static final String MODEL_FORMAT = "model_format";
    private static final String MODEL_CONFIG = "model_config";

    public RegisterAndDeployModelStep(Client client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> registerModelFuture = new CompletableFuture<>();

        MachineLearningNodeClient machineLearningNodeClient = MLClient.createMLClient((NodeClient) client);

        ActionListener<MLRegisterModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelResponse mlRegisterModelResponse) {

                /*ActionListener<MLDeployModelResponse> deployActionListener = new ActionListener<>() {
                    @Override
                    public void onResponse(MLDeployModelResponse mlDeployModelResponse) {
                        if (mlDeployModelResponse.getStatus() == MLTaskState.COMPLETED.name()) {
                            logger.info("Model deployment successful");
                            registerModelFuture.complete(
                                new WorkflowData(Map.ofEntries(Map.entry("modelId", mlRegisterModelResponse.getModelId())))
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("Model deployment failed");
                        registerModelFuture.completeExceptionally(new IOException("Model deployment failed"));
                    }
                };
                machineLearningNodeClient.deploy(mlRegisterModelResponse.getModelId(), deployActionListener);*/
                // scheduledFuture = threadPool.scheduleWithFixedDelay(new GetTask(machineLearningNodeClient,
                // mlRegisterModelResponse.getTaskId()), TimeValue.timeValueMillis(10L), ThreadPool.Names.GENERIC);

                DeployModel deployModel = new DeployModel();
                deployModel.deployModel(machineLearningNodeClient, mlRegisterModelResponse.getModelId());

            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register model");
                registerModelFuture.completeExceptionally(new IOException("Failed to register model "));
            }
        };

        FunctionName functionName = null;
        String modelName = null;
        String modelVersion = null;
        String modelGroupId = null;
        String connectorId = null;
        String description = null;
        MLModelFormat modelFormat = null;
        MLModelConfig modelConfig = null;

        for (WorkflowData workflowData : data) {
            Map<String, String> parameters = workflowData.getParams();
            Map<String, Object> content = workflowData.getContent();
            logger.info("Previous step sent params: {}, content: {}", parameters, content);

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case FUNCTION_NAME:
                        functionName = (FunctionName) content.get(FUNCTION_NAME);
                        break;
                    case MODEL_NAME:
                        modelName = (String) content.get(MODEL_NAME);
                        break;
                    case MODEL_VERSION:
                        modelVersion = (String) content.get(MODEL_VERSION);
                        break;
                    case MODEL_GROUP_ID:
                        modelGroupId = (String) content.get(MODEL_GROUP_ID);
                        break;
                    case MODEL_FORMAT:
                        modelFormat = (MLModelFormat) content.get(MODEL_FORMAT);
                        break;
                    case MODEL_CONFIG:
                        modelConfig = (MLModelConfig) content.get(MODEL_CONFIG);
                        break;
                    case DESCRIPTION:
                        description = (String) content.get(DESCRIPTION);
                        break;
                    case CONNECTOR_ID:
                        connectorId = (String) content.get(CONNECTOR_ID);
                        break;
                    default:
                        break;

                }
            }
        }

        if (Stream.of(functionName, modelName, modelVersion, modelGroupId, description, connectorId).allMatch(x -> x != null)) {

            // TODO: Add model Config and type cast correctly
            MLRegisterModelInput mlInput = MLRegisterModelInput.builder()
                .functionName(functionName)
                .modelName(modelName)
                .version(modelVersion)
                .modelGroupId(modelGroupId)
                .modelFormat(modelFormat)
                .modelConfig(modelConfig)
                .description(description)
                .connectorId(connectorId)
                .build();

            machineLearningNodeClient.register(mlInput, actionListener);
        }

        return registerModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
