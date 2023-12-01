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
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.flowframework.common.CommonValue.DEFAULT_MAPPING_OPTION;

/**
 * Step to create an index
 */
public class CreateIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexStep.class);
    private final ClusterService clusterService;
    private final Client client;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    static final String NAME = WorkflowResources.CREATE_INDEX.getWorkflowStep();
    static Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();

    /**
     * Instantiate this class
     *
     * @param clusterService The OpenSearch cluster service
     * @param client Client to create an index
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public CreateIndexStep(ClusterService clusterService, Client client, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        this.clusterService = clusterService;
        this.client = client;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {
        CompletableFuture<WorkflowData> createIndexFuture = new CompletableFuture<>();
        ActionListener<CreateIndexResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                try {
                    String resourceName = WorkflowResources.getResourceByWorkflowStep(getName());
                    logger.info("created index: {}", createIndexResponse.index());
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        createIndexResponse.index(),
                        ActionListener.wrap(response -> {
                            logger.info("successfully updated resource created in state index: {}", response.getIndex());
                            createIndexFuture.complete(
                                new WorkflowData(
                                    Map.of(resourceName, createIndexResponse.index()),
                                    currentNodeInputs.getWorkflowId(),
                                    currentNodeId
                                )
                            );
                        }, exception -> {
                            logger.error("Failed to update new created resource", exception);
                            createIndexFuture.completeExceptionally(
                                new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                            );
                        })
                    );
                } catch (Exception e) {
                    logger.error("Failed to parse and update new created resource", e);
                    createIndexFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to create an index", e);
                createIndexFuture.completeExceptionally(e);
            }
        };

        String index = null;
        String defaultMappingOption = null;
        Settings settings = null;

        // TODO: Recreating the list to get this compiling
        // Need to refactor the below iteration to pull directly from the maps
        List<WorkflowData> data = new ArrayList<>();
        data.add(currentNodeInputs);
        data.addAll(outputs.values());

        try {
            for (WorkflowData workflowData : data) {
                Map<String, Object> content = workflowData.getContent();
                index = (String) content.get(WorkflowResources.getResourceByWorkflowStep(getName()));
                defaultMappingOption = (String) content.get(DEFAULT_MAPPING_OPTION);
                if (index != null && defaultMappingOption != null && settings != null) {
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to find the correct resource for the workflow step", e);
            createIndexFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
        }

        // TODO:
        // 1. Create settings based on the index settings received from content

        try {
            CreateIndexRequest request = new CreateIndexRequest(index).mapping(
                FlowFrameworkIndicesHandler.getIndexMappings("mappings/" + defaultMappingOption + ".json"),
                JsonXContent.jsonXContent.mediaType()
            );
            client.admin().indices().create(request, actionListener);
        } catch (Exception e) {
            logger.error("Failed to find the right mapping for the index", e);
        }

        return createIndexFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
