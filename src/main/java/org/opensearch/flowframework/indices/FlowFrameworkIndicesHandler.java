/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.flowframework.constant.CommonValue.*;

@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@Log4j2
public class FlowFrameworkIndicesHandler {

    ClusterService clusterService;
    Client client;
    private static final Map<String, Object> indexSettings = Map.of("index.auto_expand_replicas", "0-1");
    private static final Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();

    static {
        for (FlowFrameworkIndex flowFrameworkIndex : FlowFrameworkIndex.values()) {
            indexMappingUpdated.put(flowFrameworkIndex.getIndexName(), new AtomicBoolean(false));
        }
    }

    public void initGlobalContextIndexIfAbsent(ActionListener<Boolean> listener) {
        initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);
    }

    public void initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex index, ActionListener<Boolean> listener) {
        String indexName = index.getIndexName();
        String mapping = index.getMapping();

        try (ThreadContext.StoredContext threadContext = client.threadPool().getThreadContext().stashContext()) {
            ActionListener<Boolean> internalListener = ActionListener.runBefore(listener, () -> threadContext.restore());
            if (!clusterService.state().metadata().hasIndex(indexName)) {
                ActionListener<CreateIndexResponse> actionListener = ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        log.info("create index:{}", indexName);
                        internalListener.onResponse(true);
                    } else {
                        internalListener.onResponse(false);
                    }
                }, e -> {
                    log.error("Failed to create index " + indexName, e);
                    internalListener.onFailure(e);
                });
                CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(mapping).settings(indexSettings);
                client.admin().indices().create(request, actionListener);
            } else {
                log.debug("index:{} is already created", indexName);
                if (indexMappingUpdated.containsKey(indexName) && !indexMappingUpdated.get(indexName).get()) {
                    shouldUpdateIndex(indexName, index.getVersion(), ActionListener.wrap(r -> {
                        if (r) {
                            // return true if update index is needed
                            client
                                    .admin()
                                    .indices()
                                    .putMapping(
                                            new PutMappingRequest().indices(indexName).source(mapping, XContentType.JSON),
                                            ActionListener.wrap(response -> {
                                                if (response.isAcknowledged()) {
                                                    UpdateSettingsRequest updateSettingRequest = new UpdateSettingsRequest();
                                                    updateSettingRequest.indices(indexName).settings(indexSettings);
                                                    client
                                                            .admin()
                                                            .indices()
                                                            .updateSettings(updateSettingRequest, ActionListener.wrap(updateResponse -> {
                                                                if (response.isAcknowledged()) {
                                                                    indexMappingUpdated.get(indexName).set(true);
                                                                    internalListener.onResponse(true);
                                                                } else {
                                                                    internalListener
                                                                            .onFailure(new FlowFrameworkException("Failed to update index setting for: " + indexName));
                                                                }
                                                            }, exception -> {
                                                                log.error("Failed to update index setting for: " + indexName, exception);
                                                                internalListener.onFailure(exception);
                                                            }));
                                                } else {
                                                    internalListener.onFailure(new FlowFrameworkException("Failed to update index: " + indexName));
                                                }
                                            }, exception -> {
                                                log.error("Failed to update index " + indexName, exception);
                                                internalListener.onFailure(exception);
                                            })
                                    );
                        } else {
                            // no need to update index if it does not exist or the version is already up-to-date.
                            indexMappingUpdated.get(indexName).set(true);
                            internalListener.onResponse(true);
                        }
                    }, e -> {
                        log.error("Failed to update index mapping", e);
                        internalListener.onFailure(e);
                    }));
                } else {
                    // No need to update index if it's already updated.
                    internalListener.onResponse(true);
                }
            }
        } catch (Exception e) {
            log.error("Failed to init index " + indexName, e);
            listener.onFailure(e);
        }
    }

    /**
     * Check if we should update index based on schema version.
     * @param indexName index name
     * @param newVersion new index mapping version
     * @param listener action listener, if update index is needed, will pass true to its onResponse method
     */
    public void shouldUpdateIndex(String indexName, Integer newVersion, ActionListener<Boolean> listener) {
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
