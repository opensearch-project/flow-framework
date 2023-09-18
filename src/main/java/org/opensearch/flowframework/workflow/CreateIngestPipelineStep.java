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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;

import java.util.List;
import java.util.Map;
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
        String pipelineId = null;
        String source = null;

        // Extract required content from workflow data
        for (WorkflowData workflowData : data) {

            Map<String, String> parameters = workflowData.getParams();
            Map<String, Object> content = workflowData.getContent();
            logger.debug("Previous step sent params: {}, content: {}", parameters, content);

            if (parameters.containsKey("id")) {
                pipelineId = (String) parameters.get("id");
            }
            if (content.containsKey("source")) {
                source = (String) content.get("source");
            }
            if (pipelineId != null && source != null) {
                break;
            }
        }

        // Create PutPipelineRequest and execute
        if (pipelineId != null && source != null) {
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineId, new BytesArray(source), XContentType.JSON);
            clusterAdminClient.putPipeline(putPipelineRequest, ActionListener.wrap(response -> {

                logger.info("Created pipeline : " + putPipelineRequest.getId());

                // PutPipelineRequest returns only an AcknowledgeResponse, returning pipelineId instead
                createIngestPipelineFuture.complete(new WorkflowData() {
                    @Override
                    public Map<String, Object> getContent() {
                        return Map.of("pipelineId", putPipelineRequest.getId());
                    }
                });

                // TODO : Use node client to index response data to global context (pending global context index implementation)

            }, exception -> {
                logger.error("Failed to create pipeline : " + exception.getMessage());
                createIngestPipelineFuture.completeExceptionally(exception);
            }));
        } else {
            // Required workflow data not found
            createIngestPipelineFuture.completeExceptionally(new Exception("Failed to create pipeline"));
        }

        return createIngestPipelineFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
