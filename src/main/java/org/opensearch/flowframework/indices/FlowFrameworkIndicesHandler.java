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
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
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
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.index.engine.VersionConflictEngineException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.CONFIG_INDEX_MAPPING;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX_MAPPING;
import static org.opensearch.flowframework.common.CommonValue.META;
import static org.opensearch.flowframework.common.CommonValue.NO_SCHEMA_VERSION;
import static org.opensearch.flowframework.common.CommonValue.SCHEMA_VERSION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX_MAPPING;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * A handler for operations on system indices in the AI Flow Framework plugin
 * The current indices we have are global-context and workflow-state indices
 */
public class FlowFrameworkIndicesHandler {
    private static final Logger logger = LogManager.getLogger(FlowFrameworkIndicesHandler.class);
    private final Client client;
    private final ClusterService clusterService;
    private final EncryptorUtils encryptorUtils;
    private static final Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();
    private static final Map<String, Object> indexSettings = Map.of("index.auto_expand_replicas", "0-5");
    private final NamedXContentRegistry xContentRegistry;
    // Retries in case of simultaneous updates
    private static final int RETRIES = 5;

    /**
     * constructor
     * @param client the open search client
     * @param clusterService ClusterService
     * @param encryptorUtils encryption utility
     * @param xContentRegistry contentRegister to parse any response
     */
    public FlowFrameworkIndicesHandler(
        Client client,
        ClusterService clusterService,
        EncryptorUtils encryptorUtils,
        NamedXContentRegistry xContentRegistry
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.encryptorUtils = encryptorUtils;
        for (FlowFrameworkIndex mlIndex : FlowFrameworkIndex.values()) {
            indexMappingUpdated.put(mlIndex.getIndexName(), new AtomicBoolean(false));
        }
        this.xContentRegistry = xContentRegistry;
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
     * Get config index mapping
     * @return config index mapping
     * @throws IOException if mapping file cannot be read correctly
     */
    public static String getConfigIndexMappings() throws IOException {
        return getIndexMappings(CONFIG_INDEX_MAPPING);
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
     * Create config index if it's absent
     * @param listener The action listener
     */
    public void initConfigIndexIfAbsent(ActionListener<Boolean> listener) {
        initFlowFrameworkIndexIfAbsent(FlowFrameworkIndex.CONFIG, listener);
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
            ActionListener<Boolean> internalListener = ActionListener.runBefore(listener, threadContext::restore);
            if (!clusterService.state().metadata().hasIndex(indexName)) {
                @SuppressWarnings("deprecation")
                ActionListener<CreateIndexResponse> actionListener = ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("create index: {}", indexName);
                        internalListener.onResponse(true);
                    } else {
                        internalListener.onResponse(false);
                    }
                }, e -> {
                    String errorMessage = "Failed to create index " + indexName;
                    logger.error(errorMessage, e);
                    internalListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                });
                CreateIndexRequest request = new CreateIndexRequest(indexName).mapping("{\"_doc\":" + mapping + "}")
                    .settings(indexSettings);
                client.admin().indices().create(request, actionListener);
            } else {
                logger.debug("index: {} is already created", indexName);
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
                                                        new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception))
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
                                            new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception))
                                        );
                                    })
                                );
                        } else {
                            // no need to update index if it does not exist or the version is already up-to-date.
                            indexMappingUpdated.get(indexName).set(true);
                            internalListener.onResponse(true);
                        }
                    }, e -> {
                        String errorMessage = "Failed to update index mapping for " + indexName;
                        logger.error(errorMessage, e);
                        internalListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                    }));
                } else {
                    // No need to update index if it's already updated.
                    internalListener.onResponse(true);
                }
            }
        } catch (Exception e) {
            String errorMessage = "Failed to init index " + indexName;
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
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
        if (meta instanceof Map) {
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
        return ParseUtils.resourceToString("/" + mapping);
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
                Template templateWithEncryptedCredentials = encryptorUtils.encryptTemplateCredentials(template);
                request.source(templateWithEncryptedCredentials.toXContent(builder, ToXContent.EMPTY_PARAMS))
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                client.index(request, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                String errorMessage = "Failed to index global_context index";
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }, e -> {
            logger.error("Failed to create global_context index");
            listener.onFailure(e);
        }));
    }

    /**
     * Initializes config index and EncryptorUtils
     * @param listener action listener
     */
    public void initializeConfigIndex(ActionListener<Boolean> listener) {
        initConfigIndexIfAbsent(ActionListener.wrap(indexCreated -> {
            if (!indexCreated) {
                listener.onFailure(new FlowFrameworkException("No response to create config index", INTERNAL_SERVER_ERROR));
                return;
            }
            encryptorUtils.initializeMasterKey(listener);
        }, createIndexException -> {
            logger.error("Failed to create config index");
            listener.onFailure(createIndexException);
        }));
    }

    /**
     * add document insert into global context index
     * @param workflowId the workflowId, corresponds to document ID of
     * @param user passes the user that created the workflow
     * @param listener action listener
     */
    public void putInitialStateToWorkflowState(String workflowId, User user, ActionListener<IndexResponse> listener) {
        WorkflowState state = WorkflowState.builder()
            .workflowId(workflowId)
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
                client.index(request, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                String errorMessage = "Failed to put state index document";
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }

        }, e -> {
            String errorMessage = "Failed to create workflow_state index";
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }));
    }

    /**
     * Replaces a document in the global context index
     * @param documentId the document Id
     * @param template the use-case template
     * @param listener action listener
     */
    public void updateTemplateInGlobalContext(String documentId, Template template, ActionListener<IndexResponse> listener) {
        updateTemplateInGlobalContext(documentId, template, listener, false);
    }

    /**
     * Replaces a document in the global context index
     * @param documentId the document Id
     * @param template the use-case template
     * @param listener action listener
     * @param ignoreNotStartedCheck if set true, ignores the requirement that the provisioning is not started
     */
    public void updateTemplateInGlobalContext(
        String documentId,
        Template template,
        ActionListener<IndexResponse> listener,
        boolean ignoreNotStartedCheck
    ) {
        if (!doesIndexExist(GLOBAL_CONTEXT_INDEX)) {
            String errorMessage = "Failed to update template for workflow_id : " + documentId + ", global_context index does not exist.";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
            return;
        }
        doesTemplateExist(documentId, templateExists -> {
            if (templateExists) {
                getProvisioningProgress(documentId, progress -> {
                    if (ignoreNotStartedCheck || ProvisioningProgress.NOT_STARTED.equals(progress.orElse(null))) {
                        IndexRequest request = new IndexRequest(GLOBAL_CONTEXT_INDEX).id(documentId);
                        try (
                            XContentBuilder builder = XContentFactory.jsonBuilder();
                            ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()
                        ) {
                            Template encryptedTemplate = encryptorUtils.encryptTemplateCredentials(template);
                            request.source(encryptedTemplate.toXContent(builder, ToXContent.EMPTY_PARAMS))
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                            client.index(request, ActionListener.runBefore(listener, context::restore));
                        } catch (Exception e) {
                            String errorMessage = "Failed to update global_context entry : " + documentId;
                            logger.error(errorMessage, e);
                            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                        }
                    } else {
                        String errorMessage = "The template can not be updated unless its provisioning state is NOT_STARTED: "
                            + documentId
                            + ". Deprovision the workflow to reset the state.";
                        logger.error(errorMessage);
                        listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                    }
                }, listener);
            } else {
                String errorMessage = "Failed to get template: " + documentId;
                logger.error(errorMessage);
                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
            }
        }, listener);
    }

    /**
     * Check if the given template exists in the template index
     *
     * @param documentId document id
     * @param booleanResultConsumer a consumer based on whether the template exist
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void doesTemplateExist(String documentId, Consumer<Boolean> booleanResultConsumer, ActionListener<T> listener) {
        GetRequest getRequest = new GetRequest(GLOBAL_CONTEXT_INDEX, documentId);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            client.get(getRequest, ActionListener.wrap(response -> { booleanResultConsumer.accept(response.isExists()); }, exception -> {
                context.restore();
                String errorMessage = "Failed to get template " + documentId;
                logger.error(errorMessage);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
            }));
        } catch (Exception e) {
            String errorMessage = "Failed to retrieve template from global context: " + documentId;
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

    /**
     * Check workflow provisioning state and executes the consumer
     *
     * @param documentId document id
     * @param provisioningProgressConsumer consumer function based on if workflow is provisioned.
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getProvisioningProgress(
        String documentId,
        Consumer<Optional<ProvisioningProgress>> provisioningProgressConsumer,
        ActionListener<T> listener
    ) {
        GetRequest getRequest = new GetRequest(WORKFLOW_STATE_INDEX, documentId);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            client.get(getRequest, ActionListener.wrap(response -> {
                context.restore();
                if (!response.isExists()) {
                    provisioningProgressConsumer.accept(Optional.empty());
                    return;
                }
                try (
                    XContentParser parser = ParseUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    WorkflowState workflowState = WorkflowState.parse(parser);
                    provisioningProgressConsumer.accept(Optional.of(ProvisioningProgress.valueOf(workflowState.getProvisioningProgress())));
                } catch (Exception e) {
                    String errorMessage = "Failed to parse workflow state " + documentId;
                    logger.error(errorMessage, e);
                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }, exception -> {
                logger.error("Failed to get workflow state for {} ", documentId);
                provisioningProgressConsumer.accept(Optional.empty());
            }));
        } catch (Exception e) {
            String errorMessage = "Failed to retrieve workflow state to check provisioning status";
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

    /**
     * Check workflow provisioning state and resources to see if state can be deleted with template
     *
     * @param documentId document id
     * @param clearStatus if set true, always deletes the state document unless status is IN_PROGRESS
     * @param canDeleteStateConsumer consumer function which will be true if workflow state is not IN_PROGRESS and either no resources or true clearStatus
     * @param listener action listener from caller to fail on error
     * @param <T> action listener response type
     */
    public <T> void canDeleteWorkflowStateDoc(
        String documentId,
        boolean clearStatus,
        Consumer<Boolean> canDeleteStateConsumer,
        ActionListener<T> listener
    ) {
        GetRequest getRequest = new GetRequest(WORKFLOW_STATE_INDEX, documentId);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            client.get(getRequest, ActionListener.wrap(response -> {
                context.restore();
                if (!response.isExists()) {
                    // no need to delete if it's not there to start with
                    canDeleteStateConsumer.accept(Boolean.FALSE);
                    return;
                }
                try (
                    XContentParser parser = ParseUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    WorkflowState workflowState = WorkflowState.parse(parser);
                    canDeleteStateConsumer.accept(
                        (clearStatus || workflowState.resourcesCreated().isEmpty())
                            && !ProvisioningProgress.IN_PROGRESS.equals(
                                ProvisioningProgress.valueOf(workflowState.getProvisioningProgress())
                            )
                    );
                } catch (Exception e) {
                    String errorMessage = "Failed to parse workflow state " + documentId;
                    logger.error(errorMessage, e);
                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }, exception -> {
                logger.error("Failed to get workflow state for {} ", documentId);
                canDeleteStateConsumer.accept(Boolean.FALSE);
            }));
        } catch (Exception e) {
            String errorMessage = "Failed to retrieve workflow state to check provisioning status";
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
        }
    }

    /**
     * Updates a complete document in the workflow state index
     * @param documentId the document ID
     * @param updatedDocument a complete document to update the global state index with
     * @param listener action listener
     */
    public void updateFlowFrameworkSystemIndexDoc(
        String documentId,
        ToXContentObject updatedDocument,
        ActionListener<UpdateResponse> listener
    ) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = "Failed to update document " + documentId + " due to missing " + WORKFLOW_STATE_INDEX + " index";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                UpdateRequest updateRequest = new UpdateRequest(WORKFLOW_STATE_INDEX, documentId);
                XContentBuilder builder = XContentFactory.jsonBuilder();
                updatedDocument.toXContent(builder, null);
                updateRequest.doc(builder);
                updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                updateRequest.retryOnConflict(RETRIES);
                client.update(updateRequest, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                String errorMessage = "Failed to update " + WORKFLOW_STATE_INDEX + " entry : " + documentId;
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Updates a partial document in the workflow state index
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
            String errorMessage = "Failed to update document " + documentId + " due to missing " + WORKFLOW_STATE_INDEX + " index";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                UpdateRequest updateRequest = new UpdateRequest(WORKFLOW_STATE_INDEX, documentId);
                Map<String, Object> updatedContent = new HashMap<>(updatedFields);
                updateRequest.doc(updatedContent);
                updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                updateRequest.retryOnConflict(RETRIES);
                // TODO: decide what condition can be considered as an update conflict and add retry strategy
                client.update(updateRequest, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                String errorMessage = "Failed to update " + WORKFLOW_STATE_INDEX + " entry : " + documentId;
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Deletes a document in the workflow state index
     * @param documentId the document ID
     * @param listener action listener
     */
    public void deleteFlowFrameworkSystemIndexDoc(String documentId, ActionListener<DeleteResponse> listener) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = "Failed to delete document " + documentId + " due to missing " + WORKFLOW_STATE_INDEX + " index";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                DeleteRequest deleteRequest = new DeleteRequest(WORKFLOW_STATE_INDEX, documentId);
                deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                client.delete(deleteRequest, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                String errorMessage = "Failed to delete " + WORKFLOW_STATE_INDEX + " entry : " + documentId;
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Adds a resource to the state index, including common exception handling
     * @param currentNodeInputs Inputs to the current node
     * @param nodeId current process node (workflow step) id
     * @param workflowStepName the workflow step name that created the resource
     * @param resourceId the id of the newly created resource
     * @param listener the ActionListener for this step to handle completing the future after update
     */
    public void addResourceToStateIndex(
        WorkflowData currentNodeInputs,
        String nodeId,
        String workflowStepName,
        String resourceId,
        ActionListener<WorkflowData> listener
    ) {
        String workflowId = currentNodeInputs.getWorkflowId();
        String resourceName = getResourceByWorkflowStep(workflowStepName);
        ResourceCreated newResource = new ResourceCreated(workflowStepName, nodeId, resourceName, resourceId);
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = "Failed to update state for " + workflowId + " due to missing " + WORKFLOW_STATE_INDEX + " index";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                getAndUpdateResourceInStateDocumentWithRetries(
                    workflowId,
                    newResource,
                    OpType.INDEX,
                    RETRIES,
                    ActionListener.runBefore(listener, context::restore)
                );
            }
        }
    }

    /**
     * Removes a resource from the state index, including common exception handling
     * @param workflowId The workflow document id in the state index
     * @param resourceToDelete The resource to delete
     * @param listener the ActionListener for this step to handle completing the future after update
     */
    public void deleteResourceFromStateIndex(String workflowId, ResourceCreated resourceToDelete, ActionListener<WorkflowData> listener) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = "Failed to update state for " + workflowId + " due to missing " + WORKFLOW_STATE_INDEX + " index";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                getAndUpdateResourceInStateDocumentWithRetries(
                    workflowId,
                    resourceToDelete,
                    OpType.DELETE,
                    RETRIES,
                    ActionListener.runBefore(listener, context::restore)
                );
            }
        }
    }

    /**
     * Performs a get and update of a State Index document adding or removing a resource with strong consistency and retries
     * @param workflowId The document id to update
     * @param resource The resource to add or remove from the resources created list
     * @param operation The operation to perform on the resource (INDEX to append to the list or DELETE to remove)
     * @param retries The number of retries on update version conflicts
     * @param listener The listener to complete on success or failure
     */
    private void getAndUpdateResourceInStateDocumentWithRetries(
        String workflowId,
        ResourceCreated resource,
        OpType operation,
        int retries,
        ActionListener<WorkflowData> listener
    ) {
        GetRequest getRequest = new GetRequest(WORKFLOW_STATE_INDEX, workflowId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (!getResponse.isExists()) {
                listener.onFailure(new FlowFrameworkException("Workflow state not found for " + workflowId, RestStatus.NOT_FOUND));
                return;
            }
            WorkflowState currentState = WorkflowState.parse(getResponse.getSourceAsString());
            List<ResourceCreated> resourcesCreated = new ArrayList<>(currentState.resourcesCreated());
            if (operation == OpType.DELETE) {
                resourcesCreated.removeIf(r -> r.resourceMap().equals(resource.resourceMap()));
            } else {
                resourcesCreated.add(resource);
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            WorkflowState newState = WorkflowState.builder(currentState).resourcesCreated(resourcesCreated).build();
            newState.toXContent(builder, null);
            UpdateRequest updateRequest = new UpdateRequest(WORKFLOW_STATE_INDEX, workflowId).doc(builder)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setIfSeqNo(getResponse.getSeqNo())
                .setIfPrimaryTerm(getResponse.getPrimaryTerm());
            client.update(
                updateRequest,
                ActionListener.wrap(
                    r -> handleStateUpdateSuccess(workflowId, resource, operation, listener),
                    e -> handleStateUpdateException(workflowId, resource, operation, retries, listener, e)
                )
            );
        }, ex -> handleStateUpdateException(workflowId, resource, operation, 0, listener, ex)));
    }

    private void handleStateUpdateSuccess(
        String workflowId,
        ResourceCreated newResource,
        OpType operation,
        ActionListener<WorkflowData> listener
    ) {
        String resourceName = newResource.resourceType();
        String resourceId = newResource.resourceId();
        String nodeId = newResource.workflowStepId();
        logger.info(
            "Updated resources created for {} on step {} to {} resource {} {}",
            workflowId,
            nodeId,
            operation.equals(OpType.DELETE) ? "delete" : "add",
            resourceName,
            resourceId
        );
        listener.onResponse(new WorkflowData(Map.of(resourceName, resourceId), workflowId, nodeId));
    }

    private void handleStateUpdateException(
        String workflowId,
        ResourceCreated newResource,
        OpType operation,
        int retries,
        ActionListener<WorkflowData> listener,
        Exception e
    ) {
        if (e instanceof VersionConflictEngineException && retries > 0) {
            // Retry if we haven't exhausted retries
            getAndUpdateResourceInStateDocumentWithRetries(workflowId, newResource, operation, retries - 1, listener);
            return;
        }
        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
            "Failed to update workflow state for {} on step {} to {} resource {} {}",
            workflowId,
            newResource.workflowStepId(),
            operation.equals(OpType.DELETE) ? "delete" : "add",
            newResource.resourceType(),
            newResource.resourceId()
        ).getFormattedMessage();
        logger.error(errorMessage, e);
        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
    }
}
