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
import org.opensearch.OpenSearchException;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.transport.undeploy.MLUndeployModelsResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.SUCCESS;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;

/**
 * Step to undeploy model
 */
public class UndeployModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(UndeployModelStep.class);

    private MachineLearningNodeClient mlClient;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "undeploy_model";

    /**
     * Instantiate this class
     * @param mlClient Machine Learning client to perform the undeploy
     */
    public UndeployModelStep(MachineLearningNodeClient mlClient) {
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
        PlainActionFuture<WorkflowData> undeployModelFuture = PlainActionFuture.newFuture();

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

            mlClient.undeploy(new String[] { modelId }, null, new ActionListener<>() {
                @Override
                public void onResponse(MLUndeployModelsResponse mlUndeployModelsResponse) {
                    List<FailedNodeException> failures = mlUndeployModelsResponse.getResponse().failures();
                    if (failures.isEmpty()) {
                        undeployModelFuture.onResponse(
                            new WorkflowData(
                                Map.ofEntries(Map.entry(SUCCESS, !mlUndeployModelsResponse.getResponse().hasFailures())),
                                currentNodeInputs.getWorkflowId(),
                                currentNodeInputs.getNodeId()
                            )
                        );
                    } else {
                        List<String> failedNodes = failures.stream().map(FailedNodeException::nodeId).collect(Collectors.toList());
                        String message = "Failed to undeploy model on nodes " + failedNodes;
                        logger.error(message);
                        undeployModelFuture.onFailure(new OpenSearchException(message));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    String errorMessage = "Failed to undeploy model " + modelId;
                    logger.error(errorMessage, e);
                    undeployModelFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                }
            });
        } catch (FlowFrameworkException e) {
            undeployModelFuture.onFailure(e);
        }
        return undeployModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
