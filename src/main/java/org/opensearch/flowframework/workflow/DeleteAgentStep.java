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
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;

/**
 * Step to delete a agent for a remote model
 */
public class DeleteAgentStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(DeleteAgentStep.class);

    private MachineLearningNodeClient mlClient;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "delete_agent";

    /**
     * Instantiate this class
     * @param mlClient Machine Learning client to perform the deletion
     */
    public DeleteAgentStep(MachineLearningNodeClient mlClient) {
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
        PlainActionFuture<WorkflowData> deleteAgentFuture = PlainActionFuture.newFuture();

        ActionListener<DeleteResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                deleteAgentFuture.onResponse(
                    new WorkflowData(
                        Map.ofEntries(Map.entry(AGENT_ID, deleteResponse.getId())),
                        currentNodeInputs.getWorkflowId(),
                        currentNodeInputs.getNodeId()
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                String errorMessage = "Failed to delete agent";
                logger.error(errorMessage);
                deleteAgentFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(AGENT_ID);
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
            String agentId = (String) inputs.get(AGENT_ID);

            mlClient.deleteAgent(agentId, actionListener);
        } catch (FlowFrameworkException e) {
            deleteAgentFuture.onFailure(e);
        }
        return deleteAgentFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
