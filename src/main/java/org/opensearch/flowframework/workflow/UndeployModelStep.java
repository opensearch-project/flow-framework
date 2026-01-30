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
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to undeploy model
 */
public class UndeployModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(UndeployModelStep.class);

    private MachineLearningNodeClient mlClient;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "undeploy_model";
    /** Required input keys **/
    public static final Set<String> REQUIRED_INPUTS = Set.of(MODEL_ID);
    /** Optional input keys */
    public static final Set<String> OPTIONAL_INPUTS = Collections.emptySet();
    /** Provided output keys */
    public static final Set<String> PROVIDED_OUTPUTS = Set.of(SUCCESS);
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
        Map<String, String> params,
        String tenantId
    ) {
        PlainActionFuture<WorkflowData> undeployModelFuture = PlainActionFuture.newFuture();



        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                REQUIRED_INPUTS,
                OPTIONAL_INPUTS,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String modelId = inputs.get(MODEL_ID).toString();

            mlClient.undeploy(new String[] { modelId }, null, tenantId, new ActionListener<>() {
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
                public void onFailure(Exception ex) {
                    Exception e = getSafeException(ex);
                    String errorMessage = (e == null
                        ? ParameterizedMessageFactory.INSTANCE.newMessage("Failed to undeploy model {}", modelId).getFormattedMessage()
                        : e.getMessage());
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
