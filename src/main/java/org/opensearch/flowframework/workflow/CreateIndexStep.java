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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.flowframework.common.CommonValue.TYPE;

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
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
        CompletableFuture<WorkflowData> createIndexFuture = new CompletableFuture<>();
        ActionListener<CreateIndexResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                try {
                    String resourceName = WorkflowResources.getResourceByWorkflowStep(getName());
                    logger.info("created index: {}", createIndexResponse.index());
                    createIndexFuture.complete(
                        new WorkflowData(Map.of(resourceName, createIndexResponse.index()), data.get(0).getWorkflowId())
                    );
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        data.get(0),
                        getName(),
                        createIndexResponse.index(),
                        createIndexFuture
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
        String type = null;
        Settings settings = null;

        try {
            for (WorkflowData workflowData : data) {
                Map<String, Object> content = workflowData.getContent();
                index = (String) content.get(WorkflowResources.getResourceByWorkflowStep(getName()));
                type = (String) content.get(TYPE);
                if (index != null && type != null && settings != null) {
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
                FlowFrameworkIndicesHandler.getIndexMappings("mappings/" + type + ".json"),
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
