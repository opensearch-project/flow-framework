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
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.PIPELINE_ID;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * Step to create an ingest pipeline
 */
public class CreateIngestPipelineStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(CreateIngestPipelineStep.class);

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "create_ingest_pipeline";

    // Client to store a pipeline in the cluster state
    private final ClusterAdminClient clusterAdminClient;

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /**
     * Instantiates a new CreateIngestPipelineStepDraft
     * @param client The client to create a pipeline and store workflow data into the global context index
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public CreateIngestPipelineStep(Client client, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        this.clusterAdminClient = client.admin().cluster();
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {

        PlainActionFuture<WorkflowData> createIngestPipelineFuture = PlainActionFuture.newFuture();

        ActionListener<AcknowledgedResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                String resourceName = getResourceByWorkflowStep(getName());
                try {
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        currentNodeInputs.getContent().get(PIPELINE_ID).toString(),
                        ActionListener.wrap(updateResponse -> {
                            logger.info("successfully updated resources created in state index: {}", updateResponse.getIndex());
                            // PutPipelineRequest returns only an AcknowledgeResponse, returning pipelineId instead
                            // TODO: revisit this concept of pipeline_id to be consistent with what makes most sense to end user here
                            createIngestPipelineFuture.onResponse(
                                new WorkflowData(
                                    Map.of(resourceName, currentNodeInputs.getContent().get(PIPELINE_ID).toString()),
                                    currentNodeInputs.getWorkflowId(),
                                    currentNodeInputs.getNodeId()
                                )
                            );
                        }, exception -> {
                            String errorMessage = "Failed to update new created "
                                + currentNodeId
                                + " resource "
                                + getName()
                                + " id "
                                + currentNodeInputs.getContent().get(PIPELINE_ID).toString();
                            logger.error(errorMessage, exception);
                            createIngestPipelineFuture.onFailure(
                                new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception))
                            );
                        })
                    );

                } catch (Exception e) {
                    String errorMessage = "Failed to parse and update new created resource";
                    logger.error(errorMessage, e);
                    createIngestPipelineFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                String errorMessage = "Failed to create ingest pipeline";
                logger.error(errorMessage, e);
                createIngestPipelineFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }

        };

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

            // Regex to find patterns like ${{deploy_openai_model.model_id}}
            // We currently support one previous node input that fits the pattern of (step.input_to_look_for)
            Pattern pattern = Pattern.compile("\\$\\{\\{([\\w_]+)\\.([\\w_]+)\\}\\}");
            Matcher matcher = pattern.matcher(configurations);

            StringBuffer result = new StringBuffer();
            while (matcher.find()) {
                // Params map contains params for previous node input (e.g: deploy_openai_model:model_id)
                // Check first if the substitution is looking for the same key, value pair and if yes
                // then replace it with the key value pair in the inputs map
                if (params.containsKey(matcher.group(1)) && params.get(matcher.group(1)).equals(matcher.group(2))) {
                    // Extract the key for the inputs (e.g., "model_id" from ${{deploy_openai_model.model_id}})
                    String key = matcher.group(2);
                    if (inputs.containsKey(key)) {
                        // Replace the whole sequence with the value from the map
                        matcher.appendReplacement(result, (String) inputs.get(key));
                    }
                }
            }
            matcher.appendTail(result);

            if (result == null || pipelineId == null) {
                // Required workflow data not found
                createIngestPipelineFuture.onFailure(
                    new FlowFrameworkException(
                        "Failed to create ingest pipeline for " + currentNodeId + ", required inputs not found",
                        RestStatus.BAD_REQUEST
                    )
                );
            }

            byte[] byteArr = result.toString().getBytes(StandardCharsets.UTF_8);
            BytesReference configurationsBytes = new BytesArray(byteArr);

            // Create PutPipelineRequest and execute
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineId, configurationsBytes, XContentType.JSON);
            clusterAdminClient.putPipeline(putPipelineRequest, actionListener);

        } catch (FlowFrameworkException e) {
            createIngestPipelineFuture.onFailure(e);
        }

        return createIngestPipelineFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
