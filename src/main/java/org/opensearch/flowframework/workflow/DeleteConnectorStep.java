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
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.CONNECTOR_ID;

/**
 * Step to delete a connector for a remote model
 */
public class DeleteConnectorStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(DeleteConnectorStep.class);

    private MachineLearningNodeClient mlClient;

    static final String NAME = "delete_connector";

    /**
     * Instantiate this class
     * @param mlClient Machine Learning client to perform the deletion
     */
    public DeleteConnectorStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) throws IOException {
        CompletableFuture<WorkflowData> deleteConnectorFuture = new CompletableFuture<>();

        ActionListener<DeleteResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                deleteConnectorFuture.complete(
                    new WorkflowData(Map.ofEntries(Map.entry("connector_id", deleteResponse.getId())), data.get(0).getWorkflowId())
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete connector");
                deleteConnectorFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        Optional<String> connectorId = data.stream()
            .map(WorkflowData::getContent)
            .filter(m -> m.containsKey(CONNECTOR_ID))
            .map(m -> m.get(CONNECTOR_ID).toString())
            .findFirst();

        if (connectorId.isPresent()) {
            mlClient.deleteConnector(connectorId.get(), actionListener);
        } else {
            deleteConnectorFuture.completeExceptionally(
                new FlowFrameworkException("Required field " + CONNECTOR_ID + " is not provided", RestStatus.BAD_REQUEST)
            );
        }

        return deleteConnectorFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
