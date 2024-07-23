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
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.WorkflowResources.PIPELINE_ID;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to delete an ingest pipeline
 */
public class DeleteIngestPipelineStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(DeleteIngestPipelineStep.class);
    private final Client client;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "delete_ingest_pipeline";
    /** Required input keys */
    public static final Set<String> REQUIRED_INPUTS = Set.of(PIPELINE_ID);
    /** Optional input keys */
    public static final Set<String> OPTIONAL_INPUTS = Collections.emptySet();
    /** Provided output keys */
    public static final Set<String> PROVIDED_OUTPUTS = Set.of(PIPELINE_ID);

    /**
     * Instantiate this class
     *
     * @param client Client to delete an Ingest Pipeline
     */
    public DeleteIngestPipelineStep(Client client) {
        this.client = client;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> deletePipelineFuture = PlainActionFuture.newFuture();

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                REQUIRED_INPUTS,
                OPTIONAL_INPUTS,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String pipelineId = (String) inputs.get(PIPELINE_ID);

            DeletePipelineRequest deletePipelineRequest = new DeletePipelineRequest(pipelineId);

            client.admin().cluster().deletePipeline(deletePipelineRequest, ActionListener.wrap(acknowledgedResponse -> {
                logger.info("Deleted IngestPipeline: {}", pipelineId);
                deletePipelineFuture.onResponse(
                    new WorkflowData(
                        Map.ofEntries(Map.entry(PIPELINE_ID, pipelineId)),
                        currentNodeInputs.getWorkflowId(),
                        currentNodeInputs.getNodeId()
                    )
                );
            }, ex -> {
                Exception e = getSafeException(ex);
                String errorMessage = (e == null ? "Failed to delete the ingest pipeline " + pipelineId : e.getMessage());
                logger.error(errorMessage, e);
                deletePipelineFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }));
        } catch (Exception e) {
            deletePipelineFuture.onFailure(e);
        }
        return deletePipelineFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean allowDeleteRequired() {
        return true;
    }
}
