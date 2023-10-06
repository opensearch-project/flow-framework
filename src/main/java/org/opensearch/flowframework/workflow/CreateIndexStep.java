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
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndex;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.flowframework.common.CommonValue.META;
import static org.opensearch.flowframework.common.CommonValue.NO_SCHEMA_VERSION;
import static org.opensearch.flowframework.common.CommonValue.SCHEMA_VERSION_FIELD;

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
            CreateIndexRequest request = new CreateIndexRequest(index).mapping(
                getIndexMappings("mappings/" + type + ".json"),
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

    /**
     * Create Index if it's absent
     * @param index The index that needs to be created
     * @param listener The action listener
     */
    public void initIndexIfAbsent(FlowFrameworkIndex index, ActionListener<Boolean> listener) {
        String indexName = index.getIndexName();
        String mapping = index.getMapping();


        try (ThreadContext.StoredContext threadContext = client.threadPool().getThreadContext().stashContext()) {
            ActionListener<Boolean> internalListener = ActionListener.runBefore(listener, () -> threadContext.restore());
            if (!clusterService.state().metadata().hasIndex(indexName)) {
                @SuppressWarnings("deprecation")
                ActionListener<CreateIndexResponse> actionListener = ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("create index:{}", indexName);
                        internalListener.onResponse(true);
                    } else {
                        internalListener.onResponse(false);
                    }
                }, e -> {
                    logger.error("Failed to create index " + indexName, e);
                    internalListener.onFailure(e);
                });
                CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(mapping).settings(indexSettings);
                client.admin().indices().create(request, actionListener);
            } else {
                logger.debug("index:{} is already created", indexName);
                if (indexMappingUpdated.containsKey(indexName) && !indexMappingUpdated.get(indexName).get()) {
                    shouldUpdateIndex(indexName, index.getVersion(), ActionListener.wrap(r -> {
                        if (r) {
                            // return true if update index is needed
                            client.admin()
                                .indices()
                                .putMapping(
                                    new PutMappingRequest().indices(indexName).source(mapping, XContentType.JSON),
                                    ActionListener.wrap(response -> {
                                        if (response.isAcknowledged()) {
                                            UpdateSettingsRequest updateSettingRequest = new UpdateSettingsRequest();
                                            updateSettingRequest.indices(indexName).settings(indexSettings);
                                            client.admin()
                                                .indices()
                                                .updateSettings(updateSettingRequest, ActionListener.wrap(updateResponse -> {
                                                    if (response.isAcknowledged()) {
                                                        indexMappingUpdated.get(indexName).set(true);
                                                        internalListener.onResponse(true);
                                                    } else {
                                                        internalListener.onFailure(
                                                            new FlowFrameworkException(
                                                                "Failed to update index setting for: " + indexName,
                                                                INTERNAL_SERVER_ERROR
                                                            )
                                                        );
                                                    }
                                                }, exception -> {
                                                    logger.error("Failed to update index setting for: " + indexName, exception);
                                                    internalListener.onFailure(exception);
                                                }));
                                        } else {
                                            internalListener.onFailure(
                                                    new FlowFrameworkException("Failed to update index: " + indexName, INTERNAL_SERVER_ERROR)
                                            );
                                        }
                                    }, exception -> {
                                        logger.error("Failed to update index " + indexName, exception);
                                        internalListener.onFailure(exception);
                                    })
                                );
                        } else {
                            // no need to update index if it does not exist or the version is already up-to-date.
                            indexMappingUpdated.get(indexName).set(true);
                            internalListener.onResponse(true);
                        }
                    }, e -> {
                        logger.error("Failed to update index mapping", e);
                        internalListener.onFailure(e);
                    }));
                } else {
                    // No need to update index if it's already updated.
                    internalListener.onResponse(true);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to init index " + indexName, e);
            listener.onFailure(e);
        }
    }

    /**
     * Get index mapping json content.
     *
     * @param mapping type of the index to fetch the specific mapping file
     * @return index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getIndexMappings(String mapping) throws IOException {
        URL url = CreateIndexStep.class.getClassLoader().getResource(mapping);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * Check if we should update index based on schema version.
     * @param indexName index name
     * @param newVersion new index mapping version
     * @param listener action listener, if update index is needed, will pass true to its onResponse method
     */
    private void shouldUpdateIndex(String indexName, Integer newVersion, ActionListener<Boolean> listener) {
        IndexMetadata indexMetaData = clusterService.state().getMetadata().indices().get(indexName);
        if (indexMetaData == null) {
            listener.onResponse(Boolean.FALSE);
            return;
        }
        Integer oldVersion = NO_SCHEMA_VERSION;
        Map<String, Object> indexMapping = indexMetaData.mapping().getSourceAsMap();
        Object meta = indexMapping.get(META);
        if (meta != null && meta instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> metaMapping = (Map<String, Object>) meta;
            Object schemaVersion = metaMapping.get(SCHEMA_VERSION_FIELD);
            if (schemaVersion instanceof Integer) {
                oldVersion = (Integer) schemaVersion;
            }
        }
        listener.onResponse(newVersion > oldVersion);
    }
}
