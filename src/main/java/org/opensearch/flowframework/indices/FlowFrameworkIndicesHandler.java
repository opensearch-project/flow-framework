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
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.concurrent.ThreadContext.StoredContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
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
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.remote.metadata.common.SdkClientUtils;

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
    private final SdkClient sdkClient;
    private final ClusterService clusterService;
    private final EncryptorUtils encryptorUtils;
    private static final Map<String, AtomicBoolean> indexMappingUpdated = new HashMap<>();
    private static final Map<String, Object> indexSettings = Map.of("index.auto_expand_replicas", "0-1");
    private final NamedXContentRegistry xContentRegistry;
    // Retries in case of simultaneous updates
    private static final int RETRIES = 5;

    /**
     * constructor
     * @param client the open search client
     * @param sdkClient the remote metadata client
     * @param clusterService ClusterService
     * @param encryptorUtils encryption utility
     * @param xContentRegistry contentRegister to parse any response
     */
    public FlowFrameworkIndicesHandler(
        Client client,
        SdkClient sdkClient,
        ClusterService clusterService,
        EncryptorUtils encryptorUtils,
        NamedXContentRegistry xContentRegistry
    ) {
        this.client = client;
        this.sdkClient = sdkClient;
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
                ActionListener<CreateIndexResponse> actionListener = ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("create index: {}", indexName);
                        internalListener.onResponse(true);
                    } else {
                        internalListener.onResponse(false);
                    }
                }, e -> {
                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to create index {}", indexName)
                        .getFormattedMessage();
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
                                                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                                        "Failed to update index setting for: {}",
                                                        indexName
                                                    ).getFormattedMessage();
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
                                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                            "Failed to update index {}",
                                            indexName
                                        ).getFormattedMessage();
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
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Failed to update index mapping for {}",
                            indexName
                        ).getFormattedMessage();
                        logger.error(errorMessage, e);
                        internalListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                    }));
                } else {
                    // No need to update index if it's already updated.
                    internalListener.onResponse(true);
                }
            }
        } catch (Exception e) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to init index {}", indexName)
                .getFormattedMessage();
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
            putOrReplaceTemplateInGlobalContextIndex(null, template, listener);
        }, e -> {
            logger.error("Failed to create global_context index");
            listener.onFailure(e);
        }));
    }

    private void putOrReplaceTemplateInGlobalContextIndex(String documentId, Template template, ActionListener<IndexResponse> listener) {
        PutDataObjectRequest request = PutDataObjectRequest.builder()
            .index(GLOBAL_CONTEXT_INDEX)
            .id(documentId)
            .tenantId(template.getTenantId())
            .dataObject(encryptorUtils.encryptTemplateCredentials(template))
            .build();
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            sdkClient.putDataObjectAsync(request).whenComplete((r, throwable) -> {
                context.restore();
                if (throwable == null) {
                    try {
                        IndexResponse indexResponse = IndexResponse.fromXContent(r.parser());
                        listener.onResponse(indexResponse);
                    } catch (IOException e) {
                        String errorMessage = "Failed to parse index response";
                        logger.error(errorMessage, e);
                        listener.onFailure(new FlowFrameworkException(errorMessage, INTERNAL_SERVER_ERROR));
                    }
                } else {
                    Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                    String errorMessage = "Failed to index template in global context index";
                    logger.error(errorMessage, exception);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }
            });
        }
    }

    /**
     * Initializes config index and EncryptorUtils
     * @param tenantId the tenant id
     * @param listener action listener
     */
    public void initializeConfigIndex(String tenantId, ActionListener<Boolean> listener) {
        initConfigIndexIfAbsent(ActionListener.wrap(indexCreated -> {
            if (!indexCreated) {
                listener.onFailure(new FlowFrameworkException("No response to create config index", INTERNAL_SERVER_ERROR));
                return;
            }
            encryptorUtils.initializeMasterKey(tenantId, listener);
        }, createIndexException -> {
            logger.error("Failed to create config index");
            listener.onFailure(createIndexException);
        }));
    }

    /**
     * add document insert into global context index
     * @param workflowId the workflowId, corresponds to document ID
     * @param tenantId the tenant id
     * @param user passes the user that created the workflow
     * @param listener action listener
     */
    public void putInitialStateToWorkflowState(String workflowId, String tenantId, User user, ActionListener<IndexResponse> listener) {
        WorkflowState state = WorkflowState.builder()
            .workflowId(workflowId)
            .state(State.NOT_STARTED.name())
            .provisioningProgress(ProvisioningProgress.NOT_STARTED.name())
            .user(user)
            .resourcesCreated(Collections.emptyList())
            .userOutputs(Collections.emptyMap())
            .tenantId(tenantId)
            .build();
        initWorkflowStateIndexIfAbsent(ActionListener.wrap(indexCreated -> {
            if (!indexCreated) {
                listener.onFailure(new FlowFrameworkException("No response to create workflow_state index", INTERNAL_SERVER_ERROR));
                return;
            }
            PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
                .index(WORKFLOW_STATE_INDEX)
                .id(workflowId)
                .tenantId(tenantId)
                .dataObject(state)
                .build();
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                sdkClient.putDataObjectAsync(putRequest).whenComplete((r, throwable) -> {
                    context.restore();
                    if (throwable == null) {
                        try {
                            IndexResponse indexResponse = IndexResponse.fromXContent(r.parser());
                            listener.onResponse(indexResponse);
                        } catch (IOException e) {
                            logger.error("Failed to parse index response", e);
                            listener.onFailure(new FlowFrameworkException("Failed to parse index response", INTERNAL_SERVER_ERROR));
                        }
                    } else {
                        Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                        String errorMessage = "Failed to put state index document";
                        logger.error(errorMessage, exception);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    }
                });
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
        String tenantId = template.getTenantId();
        if (!doesIndexExist(GLOBAL_CONTEXT_INDEX)) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to update template for workflow_id : {}, global context index does not exist.",
                documentId
            ).getFormattedMessage();
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }
        doesTemplateExist(documentId, tenantId, templateExists -> {
            if (templateExists) {
                getProvisioningProgress(documentId, tenantId, progress -> {
                    if (ignoreNotStartedCheck || ProvisioningProgress.NOT_STARTED.equals(progress.orElse(null))) {
                        putOrReplaceTemplateInGlobalContextIndex(documentId, template, listener);
                    } else {
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "The template can not be updated unless its provisioning state is NOT_STARTED: {}. Deprovision the workflow to reset the state.",
                            documentId
                        ).getFormattedMessage();
                        logger.error(errorMessage);
                        listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                    }
                }, listener);
            } else {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to get template: {}", documentId)
                    .getFormattedMessage();
                logger.error(errorMessage);
                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
            }
        }, listener);
    }

    /**
     * Check if the given template exists in the template index
     *
     * @param documentId document id
     * @param tenantId tenant id
     * @param booleanResultConsumer a consumer based on whether the template exist
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void doesTemplateExist(
        String documentId,
        String tenantId,
        Consumer<Boolean> booleanResultConsumer,
        ActionListener<T> listener
    ) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getTemplate(
                documentId,
                tenantId,
                ActionListener.wrap(response -> booleanResultConsumer.accept(response.isExists()), exception -> {
                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to get template {}", documentId)
                        .getFormattedMessage();
                    logger.error(errorMessage);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }),
                context
            );
        }
    }

    /**
     * Get a template from the template index
     *
     * @param documentId document id
     * @param tenantId tenant id
     * @param listener action listener
     * @param context the thread context
     */
    public void getTemplate(String documentId, String tenantId, ActionListener<GetResponse> listener, StoredContext context) {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder()
            .index(GLOBAL_CONTEXT_INDEX)
            .id(documentId)
            .tenantId(tenantId)
            .build();
        sdkClient.getDataObjectAsync(getRequest).whenComplete((r, throwable) -> {
            context.restore();
            if (throwable == null) {
                try {
                    GetResponse getResponse = GetResponse.fromXContent(r.parser());
                    listener.onResponse(getResponse);
                } catch (IOException e) {
                    logger.error("Failed to parse get response", e);
                    listener.onFailure(new FlowFrameworkException("Failed to parse get response", INTERNAL_SERVER_ERROR));
                }
            } else {
                Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Failed to get template {}", documentId)
                    .getFormattedMessage();
                logger.error(errorMessage, exception);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
            }
        });
    }

    /**
     * Get a workflow state from the state index
     *
     * @param workflowId workflow id
     * @param tenantId tenant id
     * @param listener action listener
     * @param context the thread context
     */
    public void getWorkflowState(String workflowId, String tenantId, ActionListener<WorkflowState> listener, StoredContext context) {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder()
            .index(WORKFLOW_STATE_INDEX)
            .id(workflowId)
            .tenantId(tenantId)
            .build();
        sdkClient.getDataObjectAsync(getRequest).whenComplete((r, throwable) -> {
            context.restore();
            if (throwable == null) {
                try {
                    GetResponse getResponse = GetResponse.fromXContent(r.parser());
                    if (getResponse != null && getResponse.isExists()) {
                        try (
                            XContentParser parser = ParseUtils.createXContentParserFromRegistry(
                                xContentRegistry,
                                getResponse.getSourceAsBytesRef()
                            )
                        ) {
                            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                            WorkflowState workflowState = WorkflowState.parse(parser);
                            listener.onResponse(workflowState);
                        } catch (Exception e) {
                            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                "Failed to parse workflowState: {}",
                                getResponse.getId()
                            ).getFormattedMessage();
                            logger.error(errorMessage, e);
                            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.INTERNAL_SERVER_ERROR));
                        }
                    } else {
                        listener.onFailure(
                            new FlowFrameworkException("Fail to find workflow status of " + workflowId, RestStatus.NOT_FOUND)
                        );
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse get response", e);
                    listener.onFailure(new FlowFrameworkException("Failed to parse get response", INTERNAL_SERVER_ERROR));
                }
            } else {
                Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                if (exception instanceof IndexNotFoundException) {
                    listener.onFailure(new FlowFrameworkException("Fail to find workflow status of " + workflowId, RestStatus.NOT_FOUND));
                } else {
                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                        "Failed to get workflow status of: {}",
                        workflowId
                    ).getFormattedMessage();
                    logger.error(errorMessage, exception);
                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
                }
            }
        });
    }

    /**
     * Check workflow provisioning state and executes the consumer
     *
     * @param workflowId workflow id
     * @param tenantId tenant id
     * @param provisioningProgressConsumer consumer function based on if workflow is provisioned.
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getProvisioningProgress(
        String workflowId,
        String tenantId,
        Consumer<Optional<ProvisioningProgress>> provisioningProgressConsumer,
        ActionListener<T> listener
    ) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getWorkflowState(workflowId, tenantId, ActionListener.wrap(workflowState -> {
                provisioningProgressConsumer.accept(Optional.of(ProvisioningProgress.valueOf(workflowState.getProvisioningProgress())));
            }, exception -> {
                if (exception instanceof FlowFrameworkException
                    && ((FlowFrameworkException) exception).getRestStatus() == RestStatus.NOT_FOUND) {
                    provisioningProgressConsumer.accept(Optional.empty());
                } else {
                    listener.onFailure(exception);
                }
            }), context);
        }
    }

    /**
     * Check workflow provisioning state and resources to see if state can be deleted with template
     *
     * @param workflowId workflow id
     * @param tenantId tenant id
     * @param clearStatus if set true, always deletes the state document unless status is IN_PROGRESS
     * @param canDeleteStateConsumer consumer function which will be true if workflow state is not IN_PROGRESS and either no resources or true clearStatus
     * @param listener action listener from caller to fail on error
     * @param <T> action listener response type
     */
    public <T> void canDeleteWorkflowStateDoc(
        String workflowId,
        String tenantId,
        boolean clearStatus,
        Consumer<Boolean> canDeleteStateConsumer,
        ActionListener<T> listener
    ) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getWorkflowState(workflowId, tenantId, ActionListener.wrap(workflowState -> {
                canDeleteStateConsumer.accept(
                    (clearStatus || workflowState.resourcesCreated().isEmpty())
                        && !ProvisioningProgress.IN_PROGRESS.equals(ProvisioningProgress.valueOf(workflowState.getProvisioningProgress()))
                );
            }, exception -> {
                if (exception instanceof FlowFrameworkException
                    && ((FlowFrameworkException) exception).getRestStatus() == RestStatus.NOT_FOUND) {
                    canDeleteStateConsumer.accept(Boolean.FALSE);
                } else {
                    listener.onFailure(exception);
                }
            }), context);
        }
    }

    /**
     * Updates a complete document in the workflow state index
     * @param documentId the document ID
     * @param tenantId the tenant ID
     * @param updatedDocument a complete document to update the global state index with
     * @param listener action listener
     */
    public void updateFlowFrameworkSystemIndexDoc(
        String documentId,
        String tenantId,
        ToXContentObject updatedDocument,
        ActionListener<UpdateResponse> listener
    ) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to update document {} due to missing {} index",
                documentId,
                WORKFLOW_STATE_INDEX
            ).getFormattedMessage();
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
                    .index(WORKFLOW_STATE_INDEX)
                    .id(documentId)
                    .tenantId(tenantId)
                    .dataObject(updatedDocument)
                    .retryOnConflict(RETRIES)
                    .build();
                sdkClient.updateDataObjectAsync(updateRequest).whenComplete((r, throwable) -> {
                    context.restore();
                    if (throwable == null) {
                        UpdateResponse response;
                        try {
                            response = UpdateResponse.fromXContent(r.parser());
                            logger.info("Updated workflow state doc: {}", documentId);
                            listener.onResponse(response);
                        } catch (Exception e) {
                            logger.error("Failed to parse update response", e);
                            listener.onFailure(
                                new FlowFrameworkException("Failed to parse update response", RestStatus.INTERNAL_SERVER_ERROR)
                            );
                        }
                    } else {
                        Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Failed to update {} entry : {}",
                            WORKFLOW_STATE_INDEX,
                            documentId
                        ).getFormattedMessage();
                        logger.error(errorMessage, exception);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    }
                });
            } catch (Exception e) {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Failed to update {} entry : {}",
                    WORKFLOW_STATE_INDEX,
                    documentId
                ).getFormattedMessage();
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Updates a partial document in the workflow state index
     * @param documentId the document ID
     * @param tenantId the tenant ID
     * @param updatedFields the fields to update the global state index with
     * @param listener action listener
     */
    public void updateFlowFrameworkSystemIndexDoc(
        String documentId,
        String tenantId,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to update document {} due to missing {} index",
                documentId,
                WORKFLOW_STATE_INDEX
            ).getFormattedMessage();
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                Map<String, Object> updatedContent = new HashMap<>(updatedFields);
                UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
                    .index(WORKFLOW_STATE_INDEX)
                    .id(documentId)
                    .tenantId(tenantId)
                    .dataObject(updatedContent)
                    .retryOnConflict(RETRIES)
                    .build();
                // TODO: decide what condition can be considered as an update conflict and add retry strategy
                sdkClient.updateDataObjectAsync(updateRequest).whenComplete((r, throwable) -> {
                    context.restore();
                    if (throwable == null) {
                        try {
                            UpdateResponse response = UpdateResponse.fromXContent(r.parser());
                            logger.info("Updated workflow state doc: {}", documentId);
                            listener.onResponse(response);
                        } catch (Exception e) {
                            logger.error("Failed to parse update response", e);
                            listener.onFailure(
                                new FlowFrameworkException("Failed to parse update response", RestStatus.INTERNAL_SERVER_ERROR)
                            );
                        }
                    } else {
                        Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Failed to update {} entry : {}",
                            WORKFLOW_STATE_INDEX,
                            documentId
                        ).getFormattedMessage();
                        logger.error(errorMessage, exception);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    }
                });
            } catch (Exception e) {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Failed to update {} entry : {}",
                    WORKFLOW_STATE_INDEX,
                    documentId
                ).getFormattedMessage();
                logger.error(errorMessage, e);
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Deletes a document in the workflow state index
     * @param documentId the document ID
     * @param tenantId the tenant Id
     * @param listener action listener
     */
    public void deleteFlowFrameworkSystemIndexDoc(String documentId, String tenantId, ActionListener<DeleteResponse> listener) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to delete document {} due to missing {} index",
                documentId,
                WORKFLOW_STATE_INDEX
            ).getFormattedMessage();
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
        } else {
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
                    .index(WORKFLOW_STATE_INDEX)
                    .id(documentId)
                    .tenantId(tenantId)
                    .build();
                sdkClient.deleteDataObjectAsync(deleteRequest).whenComplete((r, throwable) -> {
                    context.restore();
                    if (throwable == null) {
                        try {
                            DeleteResponse response = DeleteResponse.fromXContent(r.parser());
                            logger.info("Deleted workflow state doc: {}", documentId);
                            listener.onResponse(response);
                        } catch (Exception e) {
                            logger.error("Failed to parse delete response", e);
                            listener.onFailure(
                                new FlowFrameworkException("Failed to parse delete response", RestStatus.INTERNAL_SERVER_ERROR)
                            );
                        }
                    } else {
                        Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Failed to delete {} entry : {}",
                            WORKFLOW_STATE_INDEX,
                            documentId
                        ).getFormattedMessage();
                        logger.error(errorMessage, exception);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    }
                });
            }
        }
    }

    /**
     * Adds a resource to the state index, including common exception handling
     * @param currentNodeInputs Inputs to the current node
     * @param nodeId current process node (workflow step) id
     * @param workflowStepName the workflow step name that created the resource
     * @param resourceId the id of the newly created resource
     * @param tenantId the tenant id
     * @param listener the ActionListener for this step to handle completing the future after update
     */
    public void addResourceToStateIndex(
        WorkflowData currentNodeInputs,
        String nodeId,
        String workflowStepName,
        String resourceId,
        String tenantId,
        ActionListener<WorkflowData> listener
    ) {
        String workflowId = currentNodeInputs.getWorkflowId();
        if (!validateStateIndexExists(workflowId, listener)) {
            return;
        }
        String resourceName = getResourceByWorkflowStep(workflowStepName);
        ResourceCreated newResource = new ResourceCreated(workflowStepName, nodeId, resourceName, resourceId);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getAndUpdateResourceInStateDocumentWithRetries(
                workflowId,
                tenantId,
                newResource,
                OpType.INDEX,
                RETRIES,
                ActionListener.runBefore(listener, context::restore)
            );
        }
    }

    /**
     * Removes a resource from the state index, including common exception handling
     * @param workflowId The workflow document id in the state index
     * @param tenantId The tenant id
     * @param resourceToDelete The resource to delete
     * @param listener the ActionListener for this step to handle completing the future after update
     */
    public void deleteResourceFromStateIndex(
        String workflowId,
        String tenantId,
        ResourceCreated resourceToDelete,
        ActionListener<WorkflowData> listener
    ) {
        if (!validateStateIndexExists(workflowId, listener)) {
            return;
        }
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            getAndUpdateResourceInStateDocumentWithRetries(
                workflowId,
                tenantId,
                resourceToDelete,
                OpType.DELETE,
                RETRIES,
                ActionListener.runBefore(listener, context::restore)
            );
        }
    }

    private boolean validateStateIndexExists(String workflowId, ActionListener<WorkflowData> listener) {
        if (!doesIndexExist(WORKFLOW_STATE_INDEX)) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to update state for {} due to missing {} index",
                workflowId,
                WORKFLOW_STATE_INDEX
            ).getFormattedMessage();
            logger.error(errorMessage);
            listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
            return false;
        }
        return true;
    }

    /**
     * Performs a get and update of a State Index document adding or removing a resource with strong consistency and retries
     * @param workflowId The document id to update
     * @param tenantId
     * @param resource The resource to add or remove from the resources created list
     * @param operation The operation to perform on the resource (INDEX to append to the list or DELETE to remove)
     * @param retries The number of retries on update version conflicts
     * @param listener The listener to complete on success or failure
     */
    private void getAndUpdateResourceInStateDocumentWithRetries(
        String workflowId,
        String tenantId,
        ResourceCreated resource,
        OpType operation,
        int retries,
        ActionListener<WorkflowData> listener
    ) {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder()
            .index(WORKFLOW_STATE_INDEX)
            .id(workflowId)
            .tenantId(tenantId)
            .build();
        sdkClient.getDataObjectAsync(getRequest).whenComplete((r, throwable) -> {
            if (throwable == null) {
                try {
                    GetResponse getResponse = GetResponse.fromXContent(r.parser());
                    handleStateGetResponse(workflowId, tenantId, resource, operation, retries, listener, getResponse);
                } catch (Exception e) {
                    logger.error("Failed to parse get response", e);
                    listener.onFailure(new FlowFrameworkException("Failed to parse get response", INTERNAL_SERVER_ERROR));
                }
            } else {
                Exception ex = SdkClientUtils.unwrapAndConvertToException(throwable);
                handleStateUpdateException(workflowId, tenantId, resource, operation, 0, listener, ex);
            }
        });
    }

    private void handleStateGetResponse(
        String workflowId,
        String tenantId,
        ResourceCreated resource,
        OpType operation,
        int retries,
        ActionListener<WorkflowData> listener,
        GetResponse getResponse
    ) {
        if (!getResponse.isExists()) {
            listener.onFailure(new FlowFrameworkException("Workflow state not found for " + workflowId, RestStatus.NOT_FOUND));
            return;
        }
        try {
            WorkflowState currentState = WorkflowState.parse(getResponse.getSourceAsString());
            List<ResourceCreated> resourcesCreated = new ArrayList<>(currentState.resourcesCreated());
            if (operation == OpType.DELETE) {
                resourcesCreated.removeIf(r -> r.resourceMap().equals(resource.resourceMap()));
            } else {
                resourcesCreated.add(resource);
            }
            WorkflowState newState = WorkflowState.builder(currentState).resourcesCreated(resourcesCreated).build();
            UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
                .index(WORKFLOW_STATE_INDEX)
                .id(workflowId)
                .tenantId(tenantId)
                .dataObject(newState)
                .ifSeqNo(getResponse.getSeqNo())
                .ifPrimaryTerm(getResponse.getPrimaryTerm())
                .build();
            sdkClient.updateDataObjectAsync(updateRequest).whenComplete((r, throwable) -> {
                if (throwable == null) {
                    handleStateUpdateSuccess(workflowId, resource, operation, listener);
                } else {
                    Exception e = SdkClientUtils.unwrapAndConvertToException(throwable);
                    handleStateUpdateException(workflowId, tenantId, resource, operation, retries, listener, e);
                }
            });
        } catch (Exception e) {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to parse workflow state response for {}",
                workflowId
            ).getFormattedMessage();
            logger.error(errorMessage, e);
            listener.onFailure(new FlowFrameworkException(errorMessage, INTERNAL_SERVER_ERROR));
        }
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
        String tenantId,
        ResourceCreated newResource,
        OpType operation,
        int retries,
        ActionListener<WorkflowData> listener,
        Exception e
    ) {
        if (e instanceof OpenSearchStatusException && ((OpenSearchStatusException) e).status() == RestStatus.CONFLICT && retries > 0) {
            // Retry if we haven't exhausted retries
            getAndUpdateResourceInStateDocumentWithRetries(workflowId, tenantId, newResource, operation, retries - 1, listener);
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
