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
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Workflow step to create an ingest pipeline
 */
public class CreateIngestPipelineStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIngestPipelineStep.class);
    private static final String NAME = "create_ingest_pipeline_step";

    // Client to store a pipeline in the cluster state
    private final ClusterAdminClient clusterAdminClient;
    // Client to store response data into global context index
    private final Client client;

    public CreateIngestPipelineStep(Client client) {
        this.clusterAdminClient = client.admin().cluster();
        this.client = client;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> createIngestPipelineFuture = new CompletableFuture<>();
        PutPipelineRequest putPipelineRequest = null;

        // TODO : Still not clear if this is the correct way to retrieve data from the parsed use case tempalte
        for (WorkflowData workflowData : data) {
            if (workflowData instanceof WorkflowInputData) {

                WorkflowInputData inputData = (WorkflowInputData) workflowData;
                logger.debug("Previous step sent params: {}, content: {}", inputData.getParams(), workflowData.getContent());

                // Extract params and content to create request
                String pipelineId = inputData.getParams().get("id");
                BytesReference source = (BytesReference) inputData.getContent().get("source");
                MediaType mediaType = (MediaType) inputData.getContent().get("mediaType");
                putPipelineRequest = new PutPipelineRequest(pipelineId, source, mediaType);
            }
        }

        if (putPipelineRequest != null) {
            String pipelineId = putPipelineRequest.getId();
            clusterAdminClient.putPipeline(putPipelineRequest, ActionListener.wrap(response -> {

                // Return create pipeline response to workflow data
                logger.info("Created pipeline : " + pipelineId);
                CreateIngestPipelineResponseData responseData = new CreateIngestPipelineResponseData(pipelineId);
                createIngestPipelineFuture.complete(responseData);

                // TODO : Use node client to index response data to global context (pending global context index implementation)
            }, exception -> {
                logger.error("Failed to create pipeline : " + exception.getMessage());
                createIngestPipelineFuture.completeExceptionally(exception);
            }));
        } else {
            createIngestPipelineFuture.completeExceptionally(new Exception("Failed to create pipeline"));
        }
        return createIngestPipelineFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
