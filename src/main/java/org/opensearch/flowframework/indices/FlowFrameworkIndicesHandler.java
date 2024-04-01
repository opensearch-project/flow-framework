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
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.DynamoDbUtil.DDBClient;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.transport.GetWorkflowStateAction;
import org.opensearch.flowframework.transport.GetWorkflowStateRequest;
import org.opensearch.flowframework.util.EncryptorUtils;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.CONFIG_INDEX_MAPPING;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX_MAPPING;
import static org.opensearch.flowframework.common.CommonValue.META;
import static org.opensearch.flowframework.common.CommonValue.NO_SCHEMA_VERSION;
import static org.opensearch.flowframework.common.CommonValue.RESOURCES_CREATED_FIELD;
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
    private final DDBClient ddbClient;
    private final ClusterService clusterService;
    private final EncryptorUtils encryptorUtils;
    private static final Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();
    private static final Map<String, Object> indexSettings = Map.of("index.auto_expand_replicas", "0-1");
    private final NamedXContentRegistry xContentRegistry;

    /**
     * constructor
     * @param client the open search client
     * @param ddbClient the DynamoDB client
     * @param clusterService ClusterService
     * @param encryptorUtils encryption utility
     * @param xContentRegistry contentRegister to parse any response
     */
    public FlowFrameworkIndicesHandler(
        Client client,
        DDBClient ddbClient,
        ClusterService clusterService,
        EncryptorUtils encryptorUtils,
        NamedXContentRegistry xContentRegistry
    ) {
        this.client = client;
        this.ddbClient = ddbClient;
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
        return DynamoDbUtil.USE_DYNAMODB ? DynamoDbUtil.tableExists(indexName) : clusterService.state().metadata().hasIndex(indexName);
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
            if (!doesIndexExist(indexName)) {
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
                CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(mapping).settings(indexSettings);
                ddbClient.admin().indices().create(request, actionListener);
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
        if (DynamoDbUtil.USE_DYNAMODB) {
            listener.onResponse(Boolean.FALSE);
            return;
        }
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
                ddbClient.index(request, ActionListener.runBefore(listener, context::restore));
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
                ddbClient.index(request, ActionListener.runBefore(listener, context::restore));
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
                isWorkflowNotStarted(documentId, workflowIsNotStarted -> {
                    if (workflowIsNotStarted || ignoreNotStartedCheck) {
                        IndexRequest request = new IndexRequest(GLOBAL_CONTEXT_INDEX).id(documentId);
                        try (
                            XContentBuilder builder = XContentFactory.jsonBuilder();
                            ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()
                        ) {
                            Template encryptedTemplate = encryptorUtils.encryptTemplateCredentials(template);
                            request.source(encryptedTemplate.toXContent(builder, ToXContent.EMPTY_PARAMS))
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                            ddbClient.index(request, ActionListener.runBefore(listener, context::restore));
                        } catch (Exception e) {
                            String errorMessage = "Failed to update global_context entry : " + documentId;
                            logger.error(errorMessage, e);
                            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                        }
                    } else {
                        String errorMessage = "The template has already been provisioned so it can't be updated: " + documentId;
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
            ddbClient.get(getRequest, ActionListener.wrap(response -> { booleanResultConsumer.accept(response.isExists()); }, exception -> {
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
     * Check if the workflow has been provisioned and executes the consumer by passing a boolean
     *
     * @param documentId document id
     * @param booleanResultConsumer boolean consumer function based on if workflow is provisioned or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void isWorkflowNotStarted(String documentId, Consumer<Boolean> booleanResultConsumer, ActionListener<T> listener) {
        GetRequest getRequest = new GetRequest(WORKFLOW_STATE_INDEX, documentId);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            ddbClient.get(getRequest, ActionListener.wrap(response -> {
                context.restore();
                if (!response.isExists()) {
                    booleanResultConsumer.accept(false);
                    return;
                }
                try (
                    XContentParser parser = ParseUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    WorkflowState workflowState = WorkflowState.parse(parser);
                    booleanResultConsumer.accept(workflowState.getProvisioningProgress().equals(ProvisioningProgress.NOT_STARTED.name()));
                } catch (Exception e) {
                    String errorMessage = "Failed to parse workflow state " + documentId;
                    logger.error(errorMessage, e);
                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR));
                }
            }, exception -> {
                logger.error("Failed to get workflow state for {} ", documentId);
                booleanResultConsumer.accept(false);
            }));
        } catch (Exception e) {
            String errorMessage = "Failed to retrieve workflow state to check provisioning status";
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
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
            String errorMessage = "Failed to update document for given workflow due to missing " + WORKFLOW_STATE_INDEX + " index";
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                // For DynamoDB need full state index
                if (DynamoDbUtil.USE_DYNAMODB) {
                    GetWorkflowStateRequest getWorkflowRequest = new GetWorkflowStateRequest(documentId, true);
                    client.execute(GetWorkflowStateAction.INSTANCE, getWorkflowRequest, ActionListener.wrap(response -> {
                        WorkflowState state = new WorkflowState.Builder(response.getWorkflowState()).updateFields(updatedFields).build();
                        XContentBuilder builder = XContentFactory.jsonBuilder();
                        IndexRequest request = new IndexRequest(WORKFLOW_STATE_INDEX).id(documentId)
                            .source(state.toXContent(builder, ToXContent.EMPTY_PARAMS));
                        ddbClient.index(request, ActionListener.wrap(indexResponse -> {
                            ShardId shardId = new ShardId(WORKFLOW_STATE_INDEX, WORKFLOW_STATE_INDEX, 0);
                            listener.onResponse(new UpdateResponse(shardId, documentId, 0, 0, 0, Result.UPDATED));
                        }, indexEx -> {
                            String errorMessage = "Failed to update " + WORKFLOW_STATE_INDEX + " entry : " + documentId;
                            logger.error(errorMessage, indexEx);
                            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(indexEx)));
                        }));
                    }, ex -> {
                        String errorMessage = "Failed to get existing " + WORKFLOW_STATE_INDEX + " entry: " + documentId;
                        logger.error(errorMessage, ex);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(ex)));
                    }));
                } else {
                    UpdateRequest updateRequest = new UpdateRequest(WORKFLOW_STATE_INDEX, documentId);
                    Map<String, Object> updatedContent = new HashMap<>(updatedFields);
                    updateRequest.doc(updatedContent);
                    updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    updateRequest.retryOnConflict(5);
                    // TODO: decide what condition can be considered as an update conflict and add retry strategy
                    client.update(updateRequest, ActionListener.runBefore(listener, context::restore));
                }
            } catch (Exception e) {
                String errorMessage = "Failed to update " + WORKFLOW_STATE_INDEX + " entry : " + documentId;
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }

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
            String errorMessage = "Failed to update document for given workflow due to missing " + indexName + " index";
            logger.error(errorMessage);
            listener.onFailure(new Exception(errorMessage));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                UpdateRequest updateRequest = new UpdateRequest(indexName, documentId);
                // TODO: Also add ability to change other fields at the same time when adding detailed provision progress
                updateRequest.script(script);
                updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                updateRequest.retryOnConflict(3);
                // TODO: Implement our own concurrency control to improve on retry mechanism
                client.update(updateRequest, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                String errorMessage = "Failed to update " + indexName + " entry : " + documentId;
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * DynamoDB version of {@link #updateFlowFrameworkSystemIndexDocWithScript(String, String, Script, ActionListener)} for the "add a new resource" use case.
     * @param documentId the document id (workflow id) to add the resource to the state index
     * @param newResource the resource to add
     * @param listener action listener
     */
    public void updateFlowFrameworkSystemIndexDocWithNewResource(
        String documentId,
        ResourceCreated newResource,
        ActionListener<UpdateResponse> listener
    ) {
        GetWorkflowStateRequest getWorkflowRequest = new GetWorkflowStateRequest(documentId, true);
        client.execute(GetWorkflowStateAction.INSTANCE, getWorkflowRequest, ActionListener.wrap(response -> {
            List<ResourceCreated> resourcesCreated = new ArrayList<>(response.getWorkflowState().resourcesCreated());
            resourcesCreated.add(newResource);
            WorkflowState state = new WorkflowState.Builder(response.getWorkflowState()).updateFields(
                Map.of(RESOURCES_CREATED_FIELD, resourcesCreated)
            ).build();
            XContentBuilder builder = XContentFactory.jsonBuilder();
            IndexRequest request = new IndexRequest(WORKFLOW_STATE_INDEX).id(documentId)
                .source(state.toXContent(builder, ToXContent.EMPTY_PARAMS));
            ddbClient.index(request, ActionListener.wrap(indexResponse -> {
                ShardId shardId = new ShardId(WORKFLOW_STATE_INDEX, WORKFLOW_STATE_INDEX, 0);
                listener.onResponse(new UpdateResponse(shardId, documentId, 0, 0, 0, Result.UPDATED));
            }, indexEx -> {
                String errorMessage = "Failed to update " + WORKFLOW_STATE_INDEX + " entry : " + documentId;
                logger.error(errorMessage, indexEx);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(indexEx)));
            }));
        }, ex -> {
            String errorMessage = "Failed to get existing " + WORKFLOW_STATE_INDEX + " entry: " + documentId;
            logger.error(errorMessage, ex);
            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(ex)));
        }));
    }

    /**
     * Creates a new ResourceCreated object and a script to update the state index
     * @param workflowId workflowId for the relevant step
     * @param nodeId WorkflowData object with relevent step information
     * @param workflowStepName the workflowstep name that created the resource
     * @param resourceId the id of the newly created resource
     * @param listener the ActionListener for this step to handle completing the future after update
     * @throws IOException if parsing fails on new resource
     */
    public void updateResourceInStateIndex(
        String workflowId,
        String nodeId,
        String workflowStepName,
        String resourceId,
        ActionListener<UpdateResponse> listener
    ) throws IOException {
        ResourceCreated newResource = new ResourceCreated(
            workflowStepName,
            nodeId,
            getResourceByWorkflowStep(workflowStepName),
            resourceId
        );

        if (DynamoDbUtil.USE_DYNAMODB) {
            updateFlowFrameworkSystemIndexDocWithNewResource(workflowId, newResource, ActionListener.wrap(updateResponse -> {
                logger.info("updated resources created of {}", workflowId);
                listener.onResponse(updateResponse);
            }, listener::onFailure));
        } else {
            // The script to append a new object to the resources_created array
            Script script = new Script(
                ScriptType.INLINE,
                "painless",
                "ctx._source.resources_created.add(params.newResource)",
                Collections.singletonMap("newResource", newResource.resourceMap())
            );

            updateFlowFrameworkSystemIndexDocWithScript(WORKFLOW_STATE_INDEX, workflowId, script, ActionListener.wrap(updateResponse -> {
                logger.info("updated resources created of {}", workflowId);
                listener.onResponse(updateResponse);
            }, listener::onFailure));
        }
    }
}
