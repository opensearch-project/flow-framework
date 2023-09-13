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
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.CompletableFuture;

public class CreateIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateIndexStep.class);
    private Client client;
    private final String CREATE_INDEX_STEP = "create_index_step";

    public CreateIndexStep(Client client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(WorkflowData data) {

        ActionListener<CreateIndexResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {
                logger.info("created index:{}");
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Index creation failed", e);
            }
        };

        // Fetch indexName, fileName and settings from WorkflowData
        CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(getIndexMappings(fileName), XContentType.JSON)
            .settings(settings);
        client.admin().indices().create(request, actionListener);
        return null;
    }

    @Override
    public String getName() {
        return CREATE_INDEX_STEP;
    }

    /**
     * Get index mapping json content.
     *
     * @return index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getIndexMappings(String mappingFileName) throws IOException {
        URL url = CreateIndexStep.class.getClassLoader().getResource(mappingFileName);
        return Resources.toString(url, Charsets.UTF_8);
    }
}
