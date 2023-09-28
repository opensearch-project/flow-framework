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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Workflow step to create an ingest pipeline
 */
public class CreateIngestPipelineStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIngestPipelineStep.class);

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    static final String NAME = "create_ingest_pipeline";

    // Common pipeline configuration fields
    private static final String PIPELINE_ID_FIELD = "id";
    private static final String DESCRIPTION_FIELD = "description";
    private static final String PROCESSORS_FIELD = "processors";
    private static final String TYPE_FIELD = "type";

    // Temporary text embedding processor fields
    private static final String FIELD_MAP = "field_map";
    private static final String MODEL_ID_FIELD = "model_id";
    private static final String INPUT_FIELD = "input_field_name";
    private static final String OUTPUT_FIELD = "output_field_name";

    // Client to store a pipeline in the cluster state
    private final ClusterAdminClient clusterAdminClient;

    /**
     * Instantiates a new CreateIngestPipelineStep
     *
     * @param client The client to create a pipeline and store workflow data into the global context index
     */
    public CreateIngestPipelineStep(Client client) {
        this.clusterAdminClient = client.admin().cluster();
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> createIngestPipelineFuture = new CompletableFuture<>();

        String pipelineId = null;
        String description = null;
        String type = null;
        String modelId = null;
        String inputFieldName = null;
        String outputFieldName = null;
        BytesReference configuration = null;

        // Extract required content from workflow data and generate the ingest pipeline configuration
        for (WorkflowData workflowData : data) {

            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case PIPELINE_ID_FIELD:
                        pipelineId = (String) content.get(PIPELINE_ID_FIELD);
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) content.get(DESCRIPTION_FIELD);
                        break;
                    case TYPE_FIELD:
                        type = (String) content.get(TYPE_FIELD);
                        break;
                    case MODEL_ID_FIELD:
                        modelId = (String) content.get(MODEL_ID_FIELD);
                        break;
                    case INPUT_FIELD:
                        inputFieldName = (String) content.get(INPUT_FIELD);
                        break;
                    case OUTPUT_FIELD:
                        outputFieldName = (String) content.get(OUTPUT_FIELD);
                        break;
                    default:
                        break;
                }
            }

            // Determmine if fields have been populated, else iterate over remaining workflow data
            if (Stream.of(pipelineId, description, modelId, type, inputFieldName, outputFieldName).allMatch(x -> x != null)) {
                try {
                    configuration = BytesReference.bytes(
                        buildIngestPipelineRequestContent(description, modelId, type, inputFieldName, outputFieldName)
                    );
                } catch (IOException e) {
                    logger.error("Failed to create ingest pipeline configuration: " + e.getMessage());
                    createIngestPipelineFuture.completeExceptionally(e);
                }
                break;
            }
        }

        if (configuration == null) {
            // Required workflow data not found
            createIngestPipelineFuture.completeExceptionally(new Exception("Failed to create ingest pipeline, required inputs not found"));
        } else {
            // Create PutPipelineRequest and execute
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineId, configuration, XContentType.JSON);
            clusterAdminClient.putPipeline(putPipelineRequest, ActionListener.wrap(response -> {
                logger.info("Created ingest pipeline : " + putPipelineRequest.getId());

                // PutPipelineRequest returns only an AcknowledgeResponse, returning pipelineId instead
                createIngestPipelineFuture.complete(new WorkflowData(Map.of("pipelineId", putPipelineRequest.getId())));

                // TODO : Use node client to index response data to global context (pending global context index implementation)

            }, exception -> {
                logger.error("Failed to create ingest pipeline : " + exception.getMessage());
                createIngestPipelineFuture.completeExceptionally(exception);
            }));
        }

        return createIngestPipelineFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Temporary, generates the ingest pipeline request content for text_embedding processor from workflow data
     * {
     *  "description" : "<description>",
     *  "processors" : [
     *      {
     *          "<type>" : {
     *              "model_id" : "<model_id>",
     *              "field_map" : {
     *                  "<input_field_name>" : "<output_field_name>"
     *          }
     *      }
     *  ]
     * }
     *
     * @param description The description of the ingest pipeline configuration
     * @param modelId The ID of the model that will be used in the embedding interface
     * @param type The processor type
     * @param inputFieldName The field name used to cache text for text embeddings
     * @param outputFieldName The field name in which output text is stored
     * @throws IOException if the request content fails to be generated
     * @return the xcontent builder with the formatted ingest pipeline configuration
     */
    private XContentBuilder buildIngestPipelineRequestContent(
        String description,
        String modelId,
        String type,
        String inputFieldName,
        String outputFieldName
    ) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .field(DESCRIPTION_FIELD, description)
            .startArray(PROCESSORS_FIELD)
            .startObject()
            .startObject(type)
            .field(MODEL_ID_FIELD, modelId)
            .startObject(FIELD_MAP)
            .field(inputFieldName, outputFieldName)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();
    }
}
