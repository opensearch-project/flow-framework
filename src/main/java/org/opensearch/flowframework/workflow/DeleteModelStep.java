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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to delete a model for a remote model
 */
public class DeleteModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(DeleteModelStep.class);

    private MachineLearningNodeClient mlClient;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "delete_model";

    /**
     * Instantiate this class
     * @param mlClient Machine Learning client to perform the deletion
     */
    public DeleteModelStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> deleteModelFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(MODEL_ID);
        Set<String> optionalKeys = Collections.emptySet();

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String modelId = inputs.get(MODEL_ID).toString();

            mlClient.deleteModel(modelId, new ActionListener<>() {
                @Override
                public void onResponse(DeleteResponse deleteResponse) {
                    deleteModelFuture.onResponse(
                        new WorkflowData(
                            Map.ofEntries(Map.entry(MODEL_ID, deleteResponse.getId())),
                            currentNodeInputs.getWorkflowId(),
                            currentNodeInputs.getNodeId()
                        )
                    );
                }

                @Override
                public void onFailure(Exception ex) {
                    Exception e = getSafeException(ex);
                    String errorMessage = (e == null ? "Failed to delete model " + modelId : e.getMessage());
                    logger.error(errorMessage, e);
                    deleteModelFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                }
            });
        } catch (FlowFrameworkException e) {
            deleteModelFuture.onFailure(e);
        }
        return deleteModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
