/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.indices;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX_MAPPING;
import static org.opensearch.flowframework.common.CommonValue.META;
import static org.opensearch.flowframework.common.CommonValue.NO_SCHEMA_VERSION;
import static org.opensearch.flowframework.common.CommonValue.SCHEMA_VERSION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX_MAPPING;

/**
 * A handler for operations on system indices in the AI Flow Framework plugin
 * The current indices we have are global-context and workflow-state indices
 */
public class FlowFrameworkIndicesHandler {
    private static final Logger logger = LogManager.getLogger(FlowFrameworkIndicesHandler.class);
    private final Client client;
    private final ClusterService clusterService;
    private static final Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();
    private static final Map<String, Object> indexSettings = Map.of("index.auto_expand_replicas", "0-1");

    /**
     * constructor
     * @param client the open search client
     * @param clusterService ClusterService
     */
    public FlowFrameworkIndicesHandler(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        for (FlowFrameworkIndex mlIndex : FlowFrameworkIndex.values()) {
            indexMappingUpdated.put(mlIndex.getIndexName(), new AtomicBoolean(false));
        }
    }

    static {
        for (FlowFrameworkIndex mlIndex : FlowFrameworkIndex.values()) {
            indexMappingUpdated.put(mlIndex.getIndexName(), new AtomicBoolean(false));
        }
    }

    /**
     * Get global-context index mapping
     * @return global-context index mapping
     * @throws IOException if mapping file cannot be read correctly
     */
    public static String getGlobalContextMappings() throws IOException {
        return getIndexMappings(GLOBAL_CONTEXT_INDEX_MAPPING);
    }

    /**
     * Get workflow-state index mapping
     * @return workflow-state index mapping
     * @throws IOException if mapping file cannot be read correctly
     */
    public static String getWorkflowStateMappings() throws IOException {
        return getIndexMappings(WORKFLOW_STATE_INDEX_MAPPING);
    }

    /**
     * Create global context index if it's absent
     * @param listener The action listener
     */
    public void initGlobalContextIndexIfAbsent(ActionListener<Boolean> listener) {
        initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex.GLOBAL_CONTEXT, listener);
    }

    /**
     * Create workflow state index if it's absent
     * @param listener The action listener
     */
    public void initWorkflowStateIndexIfAbsent(ActionListener<Boolean> listener) {
        initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex.WORKFLOW_STATE, listener);
    }

    /**
     * Checks if the given index exists
     * @param indexName the name of the index
     * @return boolean indicating the existence of an index
     */
    public boolean doesIndexExist(String indexName) {
        return clusterService.state().metadata().hasIndex(indexName);
    }

    /**
     * Create Index if it's absent
     * @param index The index that needs to be created
     * @param listener The action listener
     */
    public void initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex index, ActionListener<Boolean> listener) {
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
                    internalListener.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
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
                                                    String errorMessage = "Failed to update index setting for: " + indexName;
                                                    logger.error(errorMessage, exception);
                                                    internalListener.onFailure(
                                                        new FlowFrameworkException(
                                                            errorMessage + " : " + exception.getMessage(),
                                                            ExceptionsHelper.status(exception)
                                                        )
                                                    );
                                                }));
                                        } else {
                                            internalListener.onFailure(
                                                new FlowFrameworkException("Failed to update index: " + indexName, INTERNAL_SERVER_ERROR)
                                            );
                                        }
                                    }, exception -> {
                                        String errorMessage = "Failed to update index " + indexName;
                                        logger.error(errorMessage, exception);
                                        internalListener.onFailure(
                                            new FlowFrameworkException(
                                                errorMessage + " : " + exception.getMessage(),
                                                ExceptionsHelper.status(exception)
                                            )
                                        );
                                    })
                                );
                        } else {
                            // no need to update index if it does not exist or the version is already up-to-date.
                            indexMappingUpdated.get(indexName).set(true);
                            internalListener.onResponse(true);
                        }
                    }, e -> {
                        String errorMessage = "Failed to update index mapping";
                        logger.error(errorMessage, e);
                        internalListener.onFailure(
                            new FlowFrameworkException(errorMessage + " : " + e.getMessage(), ExceptionsHelper.status(e))
                        );
                    }));
                } else {
                    // No need to update index if it's already updated.
                    internalListener.onResponse(true);
                }
            }
        } catch (Exception e) {
            String errorMessage = "Failed to init index " + indexName;
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage + " : " + e.getMessage(), ExceptionsHelper.status(e)));
        }
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

    /**
     * Get index mapping json content.
     *
     * @param mapping type of the index to fetch the specific mapping file
     * @return index mapping
     * @throws IOException IOException if mapping file can't be read correctly
     */
    public static String getIndexMappings(String mapping) throws IOException {
        URL url = FlowFrameworkIndicesHandler.class.getClassLoader().getResource(mapping);
        return Resources.toString(url, Charsets.UTF_8);
    }

    /**
     * add document insert into global context index
     * @param template the use-case template
     * @param listener action listener
     */
    public void putTemplateToGlobalContext(Template template, ActionListener<IndexResponse> listener) {
        initGlobalContextIndexIfAbsent(ActionListener.wrap(indexCreated -> {
            if (!indexCreated) {
                listener.onFailure(new FlowFrameworkException("No response to create global_context index", INTERNAL_SERVER_ERROR));
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
                String errorMessage = "Failed to index global_context index";
                logger.error(errorMessage);
                listener.onFailure(new FlowFrameworkException(errorMessage + " : " + e.getMessage(), ExceptionsHelper.status(e)));
            }
        }, e -> {
            logger.error("Failed to create global_context index", e);
            listener.onFailure(e);
        }));
    }

    /**
     * add document insert into global context index
     * @param workflowId the workflowId, corresponds to document ID of
     * @param user passes the user that created the workflow
     * @param listener action listener
     */
    public void putInitialStateToWorkflowState(String workflowId, User user, ActionListener<IndexResponse> listener) {
        WorkflowState state = new WorkflowState.Builder().workflowId(workflowId)
            .state(State.NOT_STARTED.name())
            .provisioningProgress(ProvisioningProgress.NOT_STARTED.name())
            .user(user)
            .resourcesCreated(Collections.emptyList())
            .userOutputs(Collections.emptyMap())
            .build();
        initWorkflowStateIndexIfAbsent(ActionListener.wrap(indexCreated -> {
            if (!indexCreated) {
                listener.onFailure(new FlowFrameworkException("No response to create workflow_state index", INTERNAL_SERVER_ERROR));
                return;
            }
            IndexRequest request = new IndexRequest(WORKFLOW_STATE_INDEX);
            try (
                XContentBuilder builder = XContentFactory.jsonBuilder();
                ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext();

            ) {
                request.source(state.toXContent(builder, ToXContent.EMPTY_PARAMS)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                request.id(workflowId);
                client.index(request, ActionListener.runBefore(listener, () -> context.restore()));
            } catch (Exception e) {
                String errorMessage = "Failed to put state index document";
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage + " : " + e.getMessage(), ExceptionsHelper.status(e)));
            }

        }, e -> {
            String errorMessage = "Failed to create workflow_state index";
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage + " : " + e.getMessage(), ExceptionsHelper.status(e)));
        }));
    }

    /**
     * Replaces a document in the global context index
     * @param documentId the document Id
     * @param template the use-case template
     * @param listener action listener
     */
    public void updateTemplateInGlobalContext(String documentId, Template template, ActionListener<IndexResponse> listener) {
        if (!doesIndexExist(GLOBAL_CONTEXT_INDEX)) {
            String exceptionMessage = "Failed to update template for workflow_id : "
                + documentId
                + ", global_context index does not exist.";
            logger.error(exceptionMessage);
            listener.onFailure(new FlowFrameworkException(exceptionMessage, RestStatus.BAD_REQUEST));
        } else {
            IndexRequest request = new IndexRequest(GLOBAL_CONTEXT_INDEX).id(documentId);
            try (
                XContentBuilder builder = XContentFactory.jsonBuilder();
                ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()
            ) {
                request.source(template.toXContent(builder, ToXContent.EMPTY_PARAMS))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                client.index(request, ActionListener.runBefore(listener, () -> context.restore()));
            } catch (Exception e) {
                String errorMessage = "Failed to update global_context entry : " + documentId;
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage + " : " + e.getMessage(), ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Updates a document in the workflow state index
     * @param documentId the document ID
     * @param updatedFields the fields to update the global state index with
     * @param listener action listener
     */
    public void updateFlowFrameworkSystemIndexDoc(
        String documentId,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String exceptionMessage = "Failed to update document for given workflow due to missing " + WORKFLOW_STATE_INDEX + " index";
            logger.error(exceptionMessage);
            listener.onFailure(new FlowFrameworkException(exceptionMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                UpdateRequest updateRequest = new UpdateRequest(WORKFLOW_STATE_INDEX, documentId);
                Map<String, Object> updatedContent = new HashMap<>();
                updatedContent.putAll(updatedFields);
                updateRequest.doc(updatedContent);
                updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                // TODO: decide what condition can be considered as an update conflict and add retry strategy
                client.update(updateRequest, ActionListener.runBefore(listener, () -> context.restore()));
            } catch (Exception e) {
                String errorMessage = "Failed to update " + WORKFLOW_STATE_INDEX + " entry : " + documentId;
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage + " : " + e.getMessage(), ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Creates a new ResourceCreated object and a script to update the state index
     * @param data WorkflowData object with relevent step information
     * @param workflowStepName the workflowstep name that created the resource
     * @param resourceId the id of the newly created resource
     * @param completableFuture the CompletableFuture used for this step
     */
    public void updateResourceInStateIndex(
        WorkflowData data,
        String workflowStepName,
        String resourceId,
        CompletableFuture<WorkflowData> completableFuture
    ) throws IOException {
        String workflowId = data.getWorkflowId();
        String nodeId = data.getNodeId();
        ResourceCreated newResource = new ResourceCreated(workflowStepName, nodeId, resourceId);
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        newResource.toXContent(builder, ToXContentObject.EMPTY_PARAMS);

        // The script to append a new object to the resources_created array
        Script script = new Script(
            ScriptType.INLINE,
            "painless",
            "ctx._source.resources_created.add(params.newResource)",
            Collections.singletonMap("newResource", newResource)
        );

        updateFlowFrameworkSystemIndexDocWithScript(WORKFLOW_STATE_INDEX, workflowId, script, ActionListener.wrap(updateResponse -> {
            logger.info("updated resources created of {}", workflowId);
        }, exception -> {
            completableFuture.completeExceptionally(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
            logger.error("Failed to update workflow state with newly created resource", exception);
        }));

    }

    /**
     * Updates a document in the workflow state index
     * @param indexName the index that we will be updating a document of.
     * @param documentId the document ID
     * @param script the given script to update doc
     * @param listener action listener
     */
    public void updateFlowFrameworkSystemIndexDocWithScript(
        String indexName,
        String documentId,
        Script script,
        ActionListener<UpdateResponse> listener
    ) {
        if (!doesIndexExist(indexName)) {
            String exceptionMessage = "Failed to update document for given workflow due to missing " + indexName + " index";
            logger.error(exceptionMessage);
            listener.onFailure(new Exception(exceptionMessage));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                UpdateRequest updateRequest = new UpdateRequest(indexName, documentId);
                // TODO: Also add ability to change other fields at the same time when adding detailed provision progress
                updateRequest.script(script);
                updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                // TODO: decide what condition can be considered as an update conflict and add retry strategy
                client.update(updateRequest, ActionListener.runBefore(listener, () -> context.restore()));
            } catch (Exception e) {
                logger.error("Failed to update {} entry : {}. {}", indexName, documentId, e.getMessage());
                listener.onFailure(
                    new FlowFrameworkException("Failed to update " + indexName + "entry: " + documentId, ExceptionsHelper.status(e))
                );
            }
        }
    }
}
