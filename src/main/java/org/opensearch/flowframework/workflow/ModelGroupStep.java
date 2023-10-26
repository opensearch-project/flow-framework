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
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;

/**
 * Step to register a model group
 */
public class ModelGroupStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterModelStep.class);

    private MachineLearningNodeClient mlClient;

    static final String NAME = "model_group";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public ModelGroupStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) throws IOException {

        CompletableFuture<WorkflowData> registerModelGroupFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelGroupResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelGroupResponse mlRegisterModelGroupResponse) {
                logger.info("Model group registration successful");
                registerModelGroupFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(
                            Map.entry("model_group_id", mlRegisterModelGroupResponse.getModelGroupId()),
                            Map.entry("model_group_status", mlRegisterModelGroupResponse.getStatus())
                        )
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register model group");
                registerModelGroupFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        String modelGroupName = null;
        String description = null;

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        modelGroupName = (String) content.get(NAME_FIELD);
                        break;
                    case DESCRIPTION:
                        description = (String) content.get(DESCRIPTION);
                        break;
                    default:
                        break;
                }
            }
        }

        if (Stream.of(modelGroupName, description).allMatch(x -> x != null)) {
            MLRegisterModelGroupInput mlInput = MLRegisterModelGroupInput.builder().name(modelGroupName).description(description).build();

            mlClient.registerModelGroup(mlInput, actionListener);
        } else {
            registerModelGroupFuture.completeExceptionally(
                new FlowFrameworkException("Required fields are not provided", RestStatus.BAD_REQUEST)
            );
        }

        return registerModelGroupFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
