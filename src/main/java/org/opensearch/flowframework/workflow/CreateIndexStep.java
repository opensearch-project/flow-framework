/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Step to create an index
 */
public class CreateIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexStep.class);
    private Client client;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    static final String NAME = "create_index";

    /**
     * Instantiate this class
     * @param client Client to create an index
     */
    public CreateIndexStep(Client client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {
        CompletableFuture<WorkflowData> future = new CompletableFuture<>();
        ActionListener<CreateIndexResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                logger.info("created index: {}", createIndexResponse.index());
                future.complete(new WorkflowData(Map.of("index-name", createIndexResponse.index())));
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
            index = (String) content.get("index-name");
            type = (String) content.get("type");
            if (index != null && type != null && settings != null) {
                break;
            }
        }

        // TODO:
        // 1. Create settings based on the index settings received from content

        try {
            // TODO: mapping() is deprecated
            CreateIndexRequest request = new CreateIndexRequest(index).mapping(
                getIndexMappings("mappings/" + type + ".json"),
                XContentType.JSON
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

    /**
     * Get index mapping json content.
     *
     * @param mapping type of the index to fetch the specific mapping file
     * @return index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    private static String getIndexMappings(String mapping) throws IOException {
        URL url = CreateIndexStep.class.getClassLoader().getResource(mapping);
        return Resources.toString(url, Charsets.UTF_8);
    }
}
