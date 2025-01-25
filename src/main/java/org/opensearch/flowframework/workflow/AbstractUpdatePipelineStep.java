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
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.PIPELINE_ID;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to update either a search or ingest pipeline
 */
public abstract class AbstractUpdatePipelineStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(AbstractUpdatePipelineStep.class);

    // Client to store a pipeline in the cluster state
    private final ClusterAdminClient clusterAdminClient;

    /**
     * Instantiate this class
     * @param client client to access cluster admin client
     */
    protected AbstractUpdatePipelineStep(Client client) {
        this.clusterAdminClient = client.admin().cluster();
    }

    /**
     * Executes a put search or ingest pipeline request
     * @param pipelineId the pipeline id
     * @param configuration the pipeline configuration bytes
     * @param clusterAdminClient the cluster admin client
     * @param listener listener
     */
    public abstract void executePutPipelineRequest(
        String pipelineId,
        BytesReference configuration,
        ClusterAdminClient clusterAdminClient,
        ActionListener<AcknowledgedResponse> listener
    );

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params,
        String tenantId
    ) {
        PlainActionFuture<WorkflowData> createPipelineFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(PIPELINE_ID, CONFIGURATIONS);

        // currently, we are supporting an optional param of model ID into the various processors
        Set<String> optionalKeys = Set.of(MODEL_ID);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String pipelineId = (String) inputs.get(PIPELINE_ID);
            String configurations = (String) inputs.get(CONFIGURATIONS);

            // Special case for processors that have arrays that need to have the quotes around or
            // backslashes around strings in array removed
            String transformedJsonStringForStringArray = ParseUtils.removingBackslashesAndQuotesInArrayInJsonString(configurations);

            byte[] byteArr = transformedJsonStringForStringArray.getBytes(StandardCharsets.UTF_8);
            BytesReference configurationsBytes = new BytesArray(byteArr);

            String pipelineToBeCreated = this.getName();
            ActionListener<AcknowledgedResponse> putPipelineActionListener = new ActionListener<>() {

                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {

                    // Not necessary to update state index entry since the resource ID remains unchaged
                    String resourceName = getResourceByWorkflowStep(getName());
                    logger.info("Successfully updated resource: {}", pipelineId);
                    createPipelineFuture.onResponse(
                        new WorkflowData(Map.of(resourceName, pipelineId), currentNodeInputs.getWorkflowId(), currentNodeInputs.getNodeId())
                    );
                }

                @Override
                public void onFailure(Exception ex) {
                    Exception e = getSafeException(ex);
                    String errorMessage = (e == null
                        ? ParameterizedMessageFactory.INSTANCE.newMessage("Failed step {}", pipelineToBeCreated).getFormattedMessage()
                        : e.getMessage());
                    logger.error(errorMessage, e);
                    createPipelineFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                }

            };

            executePutPipelineRequest(pipelineId, configurationsBytes, clusterAdminClient, putPipelineActionListener);

        } catch (FlowFrameworkException e) {
            createPipelineFuture.onFailure(e);
        }
        return createPipelineFuture;

    }
}
