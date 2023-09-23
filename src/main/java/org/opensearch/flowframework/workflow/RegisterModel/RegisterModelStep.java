/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow.RegisterModel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.flowframework.client.MLClient;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class RegisterModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterModelStep.class);

    private Client client;
    private final String NAME = "register_model_step";

    private static final String FUNCTION_NAME = "function_name";
    private static final String MODEL_NAME = "model_name";
    private static final String MODEL_VERSION = "model_version";
    private static final String MODEL_GROUP_ID = "model_group_id";
    private static final String MODEL_URL = "url";
    private static final String MODEL_FORMAT = "model_format";
    private static final String MODEL_CONFIG = "model_config";
    private static final String DEPLOY_MODEL = "deploy_model";
    private static final String MODEL_NODES_IDS = "model_nodes_ids";

    public RegisterModelStep(Client client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> registerModelFuture = new CompletableFuture<>();

        FunctionName functionName = null;
        String modelName = null;
        String modelVersion = null;
        String modelGroupId = null;
        String modelUrl = null;
        MLModelFormat modelFormat = null;
        String modelConfig = null;
        Boolean deployModel = null;
        String[] modelNodesId = null;

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
                    case MODEL_URL:
                        modelUrl = (String) content.get(MODEL_URL);
                        break;
                    case MODEL_FORMAT:
                        modelFormat = (MLModelFormat) content.get(MODEL_FORMAT);
                        break;
                    case MODEL_CONFIG:
                        modelConfig = (String) content.get(MODEL_CONFIG);
                        break;
                    case DEPLOY_MODEL:
                        deployModel = (Boolean) content.get(DEPLOY_MODEL);
                        break;
                    case MODEL_NODES_IDS:
                        modelNodesId = (String[]) content.get(MODEL_NODES_IDS);
                    default:
                        break;

                }
            }
        }

        if (Stream.of(functionName, modelName, modelVersion, modelGroupId, modelConfig, modelFormat, deployModel, modelNodesId)
            .allMatch(x -> x != null)) {
            MachineLearningNodeClient machineLearningNodeClient = MLClient.createMLClient((NodeClient) client);
            // TODO: Add model Config and type cast correctly
            MLRegisterModelInput mlInput = MLRegisterModelInput.builder()
                .functionName(functionName)
                .modelName(modelName)
                .version(modelVersion)
                .modelGroupId(modelGroupId)
                .url(modelUrl)
                .modelFormat(modelFormat)
                .deployModel(deployModel)
                .modelNodeIds(modelNodesId)
                .build();

            MLRegisterModelResponse mlRegisterModelResponse = machineLearningNodeClient.register(mlInput).actionGet();

            registerModelFuture.complete(new WorkflowData() {
                @Override
                public Map<String, Object> getContent() {
                    return Map.ofEntries(
                        Map.entry("taskId", mlRegisterModelResponse.getTaskId()),
                        Map.entry("status", mlRegisterModelResponse.getStatus())
                    );
                }
            });

        } else {
            logger.error("Failed to register model");
            registerModelFuture.completeExceptionally(new IOException("Failed to register model "));
        }
        return registerModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
