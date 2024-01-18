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
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.FIELD_MAP;
import static org.opensearch.flowframework.common.CommonValue.ID;
import static org.opensearch.flowframework.common.CommonValue.INPUT_FIELD_NAME;
import static org.opensearch.flowframework.common.CommonValue.OUTPUT_FIELD_NAME;
import static org.opensearch.flowframework.common.CommonValue.PROCESSORS;
import static org.opensearch.flowframework.common.CommonValue.TYPE;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * Workflow step to create an ingest pipeline
 */
public class CreateIngestPipelineStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIngestPipelineStep.class);

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "create_ingest_pipeline";

    // Client to store a pipeline in the cluster state
    private final ClusterAdminClient clusterAdminClient;

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /**
     * Instantiates a new CreateIngestPipelineStep
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
        Map<String, String> previousNodeInputs
    ) {

        PlainActionFuture<WorkflowData> createIngestPipelineFuture = PlainActionFuture.newFuture();

        String pipelineId = null;
        String description = null;
        String type = null;
        String modelId = null;
        String inputFieldName = null;
        String outputFieldName = null;
        BytesReference configuration = null;

        // TODO: Recreating the list to get this compiling
        // Need to refactor the below iteration to pull directly from the maps
        List<WorkflowData> data = new ArrayList<>();
        data.add(currentNodeInputs);
        data.addAll(outputs.values());

        // Extract required content from workflow data and generate the ingest pipeline configuration
        for (WorkflowData workflowData : data) {

            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case ID:
                        pipelineId = (String) content.get(ID);
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) content.get(DESCRIPTION_FIELD);
                        break;
                    case TYPE:
                        type = (String) content.get(TYPE);
                        break;
                    case MODEL_ID:
                        modelId = (String) content.get(MODEL_ID);
                        break;
                    case INPUT_FIELD_NAME:
                        inputFieldName = (String) content.get(INPUT_FIELD_NAME);
                        break;
                    case OUTPUT_FIELD_NAME:
                        outputFieldName = (String) content.get(OUTPUT_FIELD_NAME);
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
                    createIngestPipelineFuture.onFailure(e);
                }
                break;
            }
        }

        if (configuration == null) {
            // Required workflow data not found
            createIngestPipelineFuture.onFailure(new Exception("Failed to create ingest pipeline, required inputs not found"));
        } else {
            // Create PutPipelineRequest and execute
            PutPipelineRequest putPipelineRequest = new PutPipelineRequest(pipelineId, configuration, XContentType.JSON);
            clusterAdminClient.putPipeline(putPipelineRequest, ActionListener.wrap(response -> {
                logger.info("Created ingest pipeline : " + putPipelineRequest.getId());

                try {
                    String resourceName = getResourceByWorkflowStep(getName());
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        putPipelineRequest.getId(),
                        ActionListener.wrap(updateResponse -> {
                            logger.info("successfully updated resources created in state index: {}", updateResponse.getIndex());
                            // PutPipelineRequest returns only an AcknowledgeResponse, returning pipelineId instead
                            // TODO: revisit this concept of pipeline_id to be consistent with what makes most sense to end user here
                            createIngestPipelineFuture.onResponse(
                                new WorkflowData(
                                    Map.of(resourceName, putPipelineRequest.getId()),
                                    currentNodeInputs.getWorkflowId(),
                                    currentNodeInputs.getNodeId()
                                )
                            );
                        }, exception -> {
                            logger.error("Failed to update new created resource", exception);
                            createIngestPipelineFuture.onFailure(
                                new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                            );
                        })
                    );

                } catch (Exception e) {
                    logger.error("Failed to parse and update new created resource", e);
                    createIngestPipelineFuture.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }

            }, exception -> {
                logger.error("Failed to create ingest pipeline : " + exception.getMessage());
                createIngestPipelineFuture.onFailure(exception);
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
            .startArray(PROCESSORS)
            .startObject()
            .startObject(type)
            .field(MODEL_ID, modelId)
            .startObject(FIELD_MAP)
            .field(inputFieldName, outputFieldName)
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .endObject();
    }
}
