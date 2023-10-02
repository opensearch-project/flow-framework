/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.workflow.CreateIndexStep;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.constant.CommonValue.GLOBAL_CONTEXT_INDEX_MAPPING;
import static org.opensearch.flowframework.workflow.CreateIndexStep.getIndexMappings;

/**
 * A handler for global context related operations
 */
public class GlobalContextHandler {
    private static final Logger logger = LogManager.getLogger(GlobalContextHandler.class);
    private final Client client;
    private final CreateIndexStep createIndexStep;

    /**
     * constructor
     * @param client the open search client
     * @param createIndexStep create index step
     */
    public GlobalContextHandler(Client client, CreateIndexStep createIndexStep) {
        this.client = client;
        this.createIndexStep = createIndexStep;
    }

    /**
     * Get global-context index mapping
     * @return global-context index mapping
     * @throws IOException if mapping file cannot be read correctly
     */
    public static String getGlobalContextMappings() throws IOException {
        return getIndexMappings(GLOBAL_CONTEXT_INDEX_MAPPING);
    }

    private void initGlobalContextIndexIfAbsent(ActionListener<Boolean> listener) {
        createIndexStep.initIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);
    }

    /**
     * add document insert into global context index
     * @param template the use-case template
     * @param listener action listener
     */
    public void putTemplateToGlobalContext(Template template, ActionListener<IndexResponse> listener) {
        initGlobalContextIndexIfAbsent(ActionListener.wrap(indexCreated -> {
            if (!indexCreated) {
                listener.onFailure(new FlowFrameworkException("No response to create global_context index"));
                return;
            }
            IndexRequest request = new IndexRequest(GLOBAL_CONTEXT_INDEX);
            try (
                XContentBuilder builder = XContentFactory.jsonBuilder();
                ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()
            ) {
                request.source(template.toXContent(builder, ToXContent.EMPTY_PARAMS))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                client.index(request, ActionListener.runBefore(listener, () -> context.restore()));
            } catch (Exception e) {
                logger.error("Failed to index global_context index");
                listener.onFailure(e);
            }
        }, e -> {
            logger.error("Failed to create global_context index", e);
            listener.onFailure(e);
        }));
    }

    /**
     * Update global context index for specific fields
     * @param documentId global context index document id
     * @param updatedFields updated fields; key: field name, value: new value
     * @param listener UpdateResponse action listener
     */
    public void storeResponseToGlobalContext(
        String documentId,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        UpdateRequest updateRequest = new UpdateRequest(GLOBAL_CONTEXT_INDEX, documentId);
        Map<String, Object> updatedResponsesContext = new HashMap<>();
        updatedResponsesContext.putAll(updatedFields);
        updateRequest.doc(updatedResponsesContext);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        // TODO: decide what condition can be considered as an update conflict and add retry strategy
        client.update(updateRequest, listener);
    }
}
