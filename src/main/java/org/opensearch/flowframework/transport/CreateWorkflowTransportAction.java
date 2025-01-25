/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.util.TenantAwareHelper;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.PluginsService;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.Boolean.FALSE;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WAIT_FOR_COMPLETION_TIMEOUT;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.flowframework.util.ParseUtils.checkFilterByBackendRoles;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;
import static org.opensearch.flowframework.util.ParseUtils.getWorkflow;

/**
 * Transport Action to index or update a use case template within the Global Context
 */
public class CreateWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(CreateWorkflowTransportAction.class);

    private final WorkflowProcessSorter workflowProcessSorter;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final Client client;
    private final SdkClient sdkClient;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private final PluginsService pluginsService;
    private volatile Boolean filterByEnabled;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;

    /**
     * Instantiates a new CreateWorkflowTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param workflowProcessSorter the workflow process sorter
     * @param flowFrameworkIndicesHandler The handler for the global context index
     * @param flowFrameworkSettings Plugin settings
     * @param client The client used to make the request to OS
     * @param sdkClient the Multitenant Client
     * @param pluginsService The plugin service
     * @param clusterService the cluster service
     * @param xContentRegistry the named content registry
     * @param settings the plugin settings
     */
    @Inject
    public CreateWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        Client client,
        SdkClient sdkClient,
        PluginsService pluginsService,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings
    ) {
        super(CreateWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.workflowProcessSorter = workflowProcessSorter;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
        this.client = client;
        this.sdkClient = sdkClient;
        this.pluginsService = pluginsService;
        filterByEnabled = FILTER_BY_BACKEND_ROLES.get(settings);
        this.clusterService = clusterService;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {
        String tenantId = request.getTemplate() == null ? null : request.getTemplate().getTenantId();
        if (!TenantAwareHelper.validateTenantId(flowFrameworkSettings.isMultiTenancyEnabled(), tenantId, listener)) {
            return;
        }
        User user = getUserContext(client);
        String workflowId = request.getWorkflowId();
        try {
            resolveUserAndExecute(
                user,
                workflowId,
                tenantId,
                flowFrameworkSettings.isMultiTenancyEnabled(),
                listener,
                () -> createExecute(request, user, tenantId, listener)
            );
        } catch (Exception e) {
            logger.error("Failed to create workflow", e);
            listener.onFailure(e);
        }
    }

    /**
     * Resolve user and execute the workflow function
     * @param requestedUser the user making the request
     * @param workflowId the workflow id
     * @param tenantId the tenant id
     * @param listener the action listener
     * @param function the workflow function to execute
     */
    private void resolveUserAndExecute(
        User requestedUser,
        String workflowId,
        String tenantId,
        boolean isMultitenancyEnabled,
        ActionListener<WorkflowResponse> listener,
        Runnable function
    ) {
        try {
            // Check if user has backend roles
            // When filter by is enabled, block users creating/updating workflows who do not have backend roles.
            if (filterByEnabled == Boolean.TRUE) {
                try {
                    checkFilterByBackendRoles(requestedUser);
                } catch (FlowFrameworkException e) {
                    logger.error(e.getMessage(), e);
                    listener.onFailure(e);
                    return;
                }
            }
            if (workflowId != null) {
                // requestedUser == null means security is disabled or user is superadmin. In this case we don't need to
                // check if request user have access to the workflow or not. But we still need to get current workflow for
                // this case, so we can keep current workflow's user data.
                boolean filterByBackendRole = requestedUser == null ? false : filterByEnabled;
                // Update workflow request, check if user has permissions to update the workflow
                // Get workflow and verify backend roles
                getWorkflow(
                    requestedUser,
                    workflowId,
                    tenantId,
                    filterByBackendRole,
                    false,
                    isMultitenancyEnabled,
                    listener,
                    function,
                    client,
                    sdkClient,
                    clusterService,
                    xContentRegistry
                );
            } else {
                // Create Workflow. No need to get current workflow.
                function.run();
            }
        } catch (Exception e) {
            String errorMessage = "Failed to create or update workflow";
            if (e instanceof FlowFrameworkException) {
                listener.onFailure(e);
            } else {
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    /**
     * Execute the create or update request
     * 1. Validate workflows if requested
     * 2. Create or update global context index
     * 3. Create or update state index
     * 4. Create or update provisioning progress index
     * @param request the workflow request
     * @param user the user making the request
     * @param tenantId the tenant id
     * @param listener the action listener
     */
    private void createExecute(WorkflowRequest request, User user, String tenantId, ActionListener<WorkflowResponse> listener) {
        Instant creationTime = Instant.now();
        Template templateWithUser = new Template(
            request.getTemplate().name(),
            request.getTemplate().description(),
            request.getTemplate().useCase(),
            request.getTemplate().templateVersion(),
            request.getTemplate().compatibilityVersion(),
            request.getTemplate().workflows(),
            request.getTemplate().getUiMetadata(),
            user,
            creationTime,
            creationTime,
            null,
            tenantId
        );

        String[] validateAll = { "all" };
        if (Arrays.equals(request.getValidation(), validateAll)) {
            try {
                validateWorkflows(templateWithUser);
            } catch (Exception e) {
                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                    "Workflow validation failed for template {}",
                    templateWithUser.name()
                ).getFormattedMessage();
                logger.error(errorMessage, e);
                listener.onFailure(
                    e instanceof FlowFrameworkException ? e : new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e))
                );
                return;
            }
        }
        String workflowId = request.getWorkflowId();
        TimeValue waitForTimeCompletion;
        if (request.getParams().containsKey(WAIT_FOR_COMPLETION_TIMEOUT)) {
            waitForTimeCompletion = TimeValue.parseTimeValue(
                request.getParams().get(WAIT_FOR_COMPLETION_TIMEOUT),
                WAIT_FOR_COMPLETION_TIMEOUT
            );
        } else {
            // default to minus one indicate async execution
            waitForTimeCompletion = TimeValue.MINUS_ONE;
        }
        if (workflowId == null) {
            // This is a new workflow (POST)
            // Throttle incoming requests
            checkMaxWorkflows(
                flowFrameworkSettings.getRequestTimeout(),
                flowFrameworkSettings.getMaxWorkflows(),
                tenantId,
                ActionListener.wrap(max -> {
                    if (FALSE.equals(max)) {
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Maximum workflows limit reached: {}",
                            flowFrameworkSettings.getMaxWorkflows()
                        ).getFormattedMessage();
                        logger.error(errorMessage);
                        FlowFrameworkException ffe = new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
                        listener.onFailure(ffe);
                        return;
                    } else {
                        // Initialize config index and create new global context and state index entries
                        flowFrameworkIndicesHandler.initializeConfigIndex(tenantId, ActionListener.wrap(isInitialized -> {
                            if (FALSE.equals(isInitialized)) {
                                listener.onFailure(
                                    new FlowFrameworkException("Failed to initalize config index", RestStatus.INTERNAL_SERVER_ERROR)
                                );
                            } else {
                                // Create new global context and state index entries
                                flowFrameworkIndicesHandler.putTemplateToGlobalContext(
                                    templateWithUser,
                                    ActionListener.wrap(globalContextResponse -> {
                                        flowFrameworkIndicesHandler.putInitialStateToWorkflowState(
                                            globalContextResponse.getId(),
                                            tenantId,
                                            user,
                                            ActionListener.wrap(stateResponse -> {
                                                logger.info("Creating state workflow doc: {}", globalContextResponse.getId());
                                                if (request.isProvision()) {
                                                    WorkflowRequest workflowRequest = new WorkflowRequest(
                                                        globalContextResponse.getId(),
                                                        Template.createEmptyTemplateWithTenantId(tenantId),
                                                        request.getParams(),
                                                        waitForTimeCompletion
                                                    );
                                                    logger.info(
                                                        "Provisioning parameter is set, continuing to provision workflow {}",
                                                        globalContextResponse.getId()
                                                    );
                                                    client.execute(
                                                        ProvisionWorkflowAction.INSTANCE,
                                                        workflowRequest,
                                                        ActionListener.wrap(provisionResponse -> {
                                                            listener.onResponse(
                                                                (workflowRequest.getWaitForCompletionTimeout() == TimeValue.MINUS_ONE)
                                                                    ? new WorkflowResponse(provisionResponse.getWorkflowId())
                                                                    : new WorkflowResponse(
                                                                        provisionResponse.getWorkflowId(),
                                                                        provisionResponse.getWorkflowState()
                                                                    )
                                                            );
                                                        }, exception -> {
                                                            String errorMessage = "Provisioning failed.";
                                                            logger.error(errorMessage, exception);
                                                            if (exception instanceof FlowFrameworkException) {
                                                                listener.onFailure(exception);
                                                            } else {
                                                                listener.onFailure(
                                                                    new FlowFrameworkException(
                                                                        errorMessage,
                                                                        ExceptionsHelper.status(exception)
                                                                    )
                                                                );
                                                            }
                                                        })
                                                    );
                                                } else {
                                                    listener.onResponse(new WorkflowResponse(globalContextResponse.getId()));
                                                }
                                            }, exception -> {
                                                String errorMessage = "Failed to save workflow state";
                                                logger.error(errorMessage, exception);
                                                if (exception instanceof FlowFrameworkException) {
                                                    listener.onFailure(exception);
                                                } else {
                                                    listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                                                }
                                            })
                                        );
                                    }, exception -> {
                                        String errorMessage = "Failed to save use case template";
                                        logger.error(errorMessage, exception);
                                        if (exception instanceof FlowFrameworkException) {
                                            listener.onFailure(exception);
                                        } else {
                                            listener.onFailure(
                                                new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception))
                                            );
                                        }
                                    })
                                );
                            }
                        }, exception -> {
                            String errorMessage = "Failed to initialize config index";
                            logger.error(errorMessage, exception);
                            if (exception instanceof FlowFrameworkException) {
                                listener.onFailure(exception);
                            } else {
                                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                            }
                        }));
                    }
                }, exception -> {
                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                        "Failed to update use case template {}",
                        request.getWorkflowId()
                    ).getFormattedMessage();
                    logger.error(errorMessage, exception);
                    if (exception instanceof FlowFrameworkException) {
                        listener.onFailure(exception);
                    } else {
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    }
                })
            );
        } else {
            // This is an existing workflow (PUT)
            // Fetch existing entry for time stamps
            logger.info("Querying existing workflow from global context: {}", workflowId);
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                sdkClient.getDataObjectAsync(
                    GetDataObjectRequest.builder().index(GLOBAL_CONTEXT_INDEX).id(workflowId).tenantId(tenantId).build()
                ).whenComplete((r, throwable) -> {
                    if (throwable == null) {
                        context.restore();
                        try {
                            GetResponse getResponse = r.parser() == null ? null : GetResponse.fromXContent(r.parser());
                            if (getResponse.isExists()) {
                                handleWorkflowExists(request, templateWithUser, getResponse, waitForTimeCompletion, listener);
                            } else {
                                String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                    "Failed to retrieve template ({}) from global context.",
                                    workflowId
                                ).getFormattedMessage();
                                logger.error(errorMessage);
                                listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
                            }
                        } catch (IOException e) {
                            logger.error("Failed to parse workflow getResponse: {}", workflowId, e);
                            listener.onFailure(e);
                        }
                    } else {
                        Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Failed to retrieve template ({}) from global context.",
                            workflowId
                        ).getFormattedMessage();
                        logger.error(errorMessage, exception);
                        listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    }
                });
            }
        }
    }

    private void handleWorkflowExists(
        WorkflowRequest request,
        Template templateWithUser,
        GetResponse getResponse,
        TimeValue waitForTimeCompletion,
        ActionListener<WorkflowResponse> listener
    ) throws IOException {
        Template existingTemplate = Template.parse(getResponse.getSourceAsString());
        Template template = request.isUpdateFields()
            ? Template.updateExistingTemplate(existingTemplate, templateWithUser)
            : Template.builder(templateWithUser)
                .createdTime(existingTemplate.createdTime())
                .lastUpdatedTime(Instant.now())
                .lastProvisionedTime(existingTemplate.lastProvisionedTime())
                .tenantId(existingTemplate.getTenantId())
                .build();

        if (request.isReprovision()) {
            handleReprovision(request.getWorkflowId(), existingTemplate, template, waitForTimeCompletion, listener);
        } else {
            // Update existing entry, full document replacement
            handleFullDocUpdate(request, template, listener);
        }
    }

    private void handleReprovision(
        String workflowId,
        Template existingTemplate,
        Template template,
        TimeValue waitForTimeCompletion,
        ActionListener<WorkflowResponse> listener
    ) {
        ReprovisionWorkflowRequest reprovisionRequest = new ReprovisionWorkflowRequest(
            workflowId,
            existingTemplate,
            template,
            waitForTimeCompletion
        );
        logger.info("Reprovisioning parameter is set, continuing to reprovision workflow {}", workflowId);
        client.execute(ReprovisionWorkflowAction.INSTANCE, reprovisionRequest, ActionListener.wrap(reprovisionResponse -> {
            listener.onResponse(new WorkflowResponse(reprovisionResponse.getWorkflowId()));
        }, exception -> {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("Reprovisioning failed for workflow {}", workflowId)
                .getFormattedMessage();
            logger.error(errorMessage, exception);
            if (exception instanceof FlowFrameworkException) {
                listener.onFailure(exception);
            } else {
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
            }
        }));
    }

    private void handleFullDocUpdate(WorkflowRequest request, Template template, ActionListener<WorkflowResponse> listener) {
        final boolean isFieldUpdate = request.isUpdateFields();
        flowFrameworkIndicesHandler.updateTemplateInGlobalContext(request.getWorkflowId(), template, ActionListener.wrap(response -> {
            // Regular update, reset provisioning status, ignore state index if updating fields
            if (!isFieldUpdate) {
                flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                    request.getWorkflowId(),
                    template.getTenantId(),
                    Map.ofEntries(
                        Map.entry(STATE_FIELD, State.NOT_STARTED),
                        Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.NOT_STARTED)
                    ),
                    ActionListener.wrap(updateResponse -> {
                        logger.info("updated workflow {} state to {}", request.getWorkflowId(), State.NOT_STARTED.name());
                        listener.onResponse(new WorkflowResponse(request.getWorkflowId()));
                    }, exception -> {
                        String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                            "Failed to update workflow {} in template index",
                            request.getWorkflowId()
                        ).getFormattedMessage();
                        logger.error(errorMessage, exception);
                        if (exception instanceof FlowFrameworkException) {
                            listener.onFailure(exception);
                        } else {
                            listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                        }
                    })
                );
            } else {
                listener.onResponse(new WorkflowResponse(request.getWorkflowId()));
            }
        }, exception -> {
            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                "Failed to update use case template {}",
                request.getWorkflowId()
            ).getFormattedMessage();
            logger.error(errorMessage, exception);
            if (exception instanceof FlowFrameworkException) {
                listener.onFailure(exception);
            } else {
                listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
            }
        }), isFieldUpdate);
    }

    /**
     * Checks if the max workflows limit has been reachesd
     *  @param requestTimeOut request time out
     *  @param maxWorkflow max workflows
     *  @param internalListener listener for search request
     */
    void checkMaxWorkflows(TimeValue requestTimeOut, Integer maxWorkflow, String tenantId, ActionListener<Boolean> internalListener) {
        if (!flowFrameworkIndicesHandler.doesIndexExist(GLOBAL_CONTEXT_INDEX)) {
            internalListener.onResponse(true);
        } else {
            QueryBuilder query = QueryBuilders.matchAllQuery();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeOut);
            SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
                .indices(GLOBAL_CONTEXT_INDEX)
                .searchSourceBuilder(searchSourceBuilder)
                .tenantId(tenantId)
                .build();
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                sdkClient.searchDataObjectAsync(searchRequest).whenComplete((r, throwable) -> {
                    if (throwable == null) {
                        context.restore();
                        try {
                            SearchResponse searchResponse = SearchResponse.fromXContent(r.parser());
                            internalListener.onResponse(searchResponse.getHits().getTotalHits().value() < maxWorkflow);
                        } catch (Exception e) {
                            logger.error("Failed to parse workflow searchResponse", e);
                            internalListener.onFailure(e);
                        }
                    } else {
                        Exception exception = SdkClientUtils.unwrapAndConvertToException(throwable);
                        String errorMessage = "Unable to fetch the workflows";
                        logger.error(errorMessage, exception);
                        internalListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                    }
                });
            } catch (Exception e) {
                String errorMessage = "Unable to fetch the workflows";
                logger.error(errorMessage, e);
                internalListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    private void validateWorkflows(Template template) throws Exception {
        for (Workflow workflow : template.workflows().values()) {
            List<ProcessNode> sortedNodes = workflowProcessSorter.sortProcessNodes(
                workflow,
                null,
                Collections.emptyMap(),
                template.getTenantId()
            );
            workflowProcessSorter.validate(sortedNodes, pluginsService);
        }
    }
}
