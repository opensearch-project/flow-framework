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
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.flowframework.common.CommonValue.INDEX_NAME;
import static org.opensearch.flowframework.common.CommonValue.TYPE;

/**
 * Step to create an index
 */
public class CreateIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexStep.class);
    private ClusterService clusterService;
    private Client client;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    static final String NAME = "create_index";
    static Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();
    private static final Map<String, Object> indexSettings = Map.of("index.auto_expand_replicas", "0-1");

    /**
     * Instantiate this class
     *
     * @param clusterService The OpenSearch cluster service
     * @param client Client to create an index
     */
    public CreateIndexStep(ClusterService clusterService, Client client) {
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        ActionListener<CreateIndexResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                logger.info("created index: {}", createIndexResponse.index());
                future.complete(new WorkflowData(Map.of(INDEX_NAME, createIndexResponse.index())));
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to create an index", e);
                future.completeExceptionally(e);
            }
        };

        String index = null;
        String type = null;
        Settings settings = null;

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();
            index = (String) content.get(INDEX_NAME);
            type = (String) content.get(TYPE);
            if (index != null && type != null && settings != null) {
                break;
            }
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

        return future;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
