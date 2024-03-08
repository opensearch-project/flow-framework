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
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.PluginsService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.lang.Boolean.FALSE;
import static org.opensearch.flowframework.common.CommonValue.GLOBAL_CONTEXT_INDEX;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.util.ParseUtils.getUserContext;

/**
 * Transport Action to index or update a use case template within the Global Context
 */
public class CreateWorkflowTransportAction extends HandledTransportAction<WorkflowRequest, WorkflowResponse> {

    private final Logger logger = LogManager.getLogger(CreateWorkflowTransportAction.class);

    private final WorkflowProcessSorter workflowProcessSorter;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final Client client;
    private final FlowFrameworkSettings flowFrameworkSettings;
    private final PluginsService pluginsService;

    /**
     * Instantiates a new CreateWorkflowTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param workflowProcessSorter the workflow process sorter
     * @param flowFrameworkIndicesHandler The handler for the global context index
     * @param flowFrameworkSettings Plugin settings
     * @param client The client used to make the request to OS
     * @param pluginsService The plugin service
     */
    @Inject
    public CreateWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings,
        Client client,
        PluginsService pluginsService
    ) {
        super(CreateWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.workflowProcessSorter = workflowProcessSorter;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.flowFrameworkSettings = flowFrameworkSettings;
        this.client = client;
        this.pluginsService = pluginsService;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {

        User user = getUserContext(client);
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
            null
        );

        String[] validateAll = { "all" };
        if (Arrays.equals(request.getValidation(), validateAll)) {
            try {
                validateWorkflows(templateWithUser);
            } catch (Exception e) {
                String errorMessage = "Workflow validation failed for template " + templateWithUser.name();
                logger.error(errorMessage, e);
                listener.onFailure(
                    e instanceof FlowFrameworkException ? e : new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e))
                );
                return;
            }
        }
        String workflowId = request.getWorkflowId();
        if (workflowId == null) {
            // This is a new workflow (POST)
            // Throttle incoming requests
            checkMaxWorkflows(
                flowFrameworkSettings.getRequestTimeout(),
                flowFrameworkSettings.getMaxWorkflows(),
                ActionListener.wrap(max -> {
                    if (FALSE.equals(max)) {
                        String errorMessage = "Maximum workflows limit reached: " + flowFrameworkSettings.getMaxWorkflows();
                        logger.error(errorMessage);
                        FlowFrameworkException ffe = new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
                        listener.onFailure(ffe);
                        return;
                    } else {
                        // Initialize config index and create new global context and state index entries
                        flowFrameworkIndicesHandler.initializeConfigIndex(ActionListener.wrap(isInitialized -> {
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
                                            user,
                                            ActionListener.wrap(stateResponse -> {
                                                logger.info("Creating state workflow doc: {}", globalContextResponse.getId());
                                                if (request.isProvision()) {
                                                    WorkflowRequest workflowRequest = new WorkflowRequest(
                                                        globalContextResponse.getId(),
                                                        null
                                                    );
                                                    logger.info(
                                                        "Provisioning parameter is set, continuing to provision workflow {}",
                                                        globalContextResponse.getId()
                                                    );
                                                    client.execute(
                                                        ProvisionWorkflowAction.INSTANCE,
                                                        workflowRequest,
                                                        ActionListener.wrap(provisionResponse -> {
                                                            listener.onResponse(new WorkflowResponse(provisionResponse.getWorkflowId()));
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
                    String errorMessage = "Failed to update use case template " + request.getWorkflowId();
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
                client.get(new GetRequest(GLOBAL_CONTEXT_INDEX, workflowId), ActionListener.wrap(getResponse -> {
                    context.restore();
                    if (getResponse.isExists()) {
                        Template existingTemplate = Template.parse(getResponse.getSourceAsString());
                        // Update existing entry, full document replacement
                        Template template = new Template.Builder(templateWithUser).createdTime(existingTemplate.createdTime())
                            .lastUpdatedTime(Instant.now())
                            .lastProvisionedTime(existingTemplate.lastProvisionedTime())
                            .build();
                        flowFrameworkIndicesHandler.updateTemplateInGlobalContext(
                            request.getWorkflowId(),
                            template,
                            ActionListener.wrap(response -> {
                                flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDoc(
                                    request.getWorkflowId(),
                                    Map.ofEntries(
                                        Map.entry(STATE_FIELD, State.NOT_STARTED),
                                        Map.entry(PROVISIONING_PROGRESS_FIELD, ProvisioningProgress.NOT_STARTED)
                                    ),
                                    ActionListener.wrap(updateResponse -> {
                                        logger.info("updated workflow {} state to {}", request.getWorkflowId(), State.NOT_STARTED.name());
                                        listener.onResponse(new WorkflowResponse(request.getWorkflowId()));
                                    }, exception -> {
                                        String errorMessage = "Failed to update workflow " + request.getWorkflowId() + " in template index";
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
                            }, exception -> {
                                String errorMessage = "Failed to update use case template " + request.getWorkflowId();
                                logger.error(errorMessage, exception);
                                if (exception instanceof FlowFrameworkException) {
                                    listener.onFailure(exception);
                                } else {
                                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                                }
                            })
                        );
                    } else {
                        String errorMessage = "Failed to retrieve template (" + workflowId + ") from global context.";
                        logger.error(errorMessage);
                        listener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.NOT_FOUND));
                    }
                }, exception -> {
                    String errorMessage = "Failed to retrieve template (" + workflowId + ") from global context.";
                    logger.error(errorMessage, exception);
                    listener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }));
            }
        }
    }

    /**
     * Checks if the max workflows limit has been reachesd
     *  @param requestTimeOut request time out
     *  @param maxWorkflow max workflows
     *  @param internalListener listener for search request
     */
    void checkMaxWorkflows(TimeValue requestTimeOut, Integer maxWorkflow, ActionListener<Boolean> internalListener) {
        if (!flowFrameworkIndicesHandler.doesIndexExist(CommonValue.GLOBAL_CONTEXT_INDEX)) {
            internalListener.onResponse(true);
        } else {
            QueryBuilder query = QueryBuilders.matchAllQuery();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeOut);

            SearchRequest searchRequest = new SearchRequest(CommonValue.GLOBAL_CONTEXT_INDEX).source(searchSourceBuilder);
            try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
                logger.info("Querying existing workflows to count the max");
                client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                    internalListener.onResponse(searchResponse.getHits().getTotalHits().value < maxWorkflow);
                }, exception -> {
                    String errorMessage = "Unable to fetch the workflows";
                    logger.error(errorMessage, exception);
                    internalListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                }));
            } catch (Exception e) {
                String errorMessage = "Unable to fetch the workflows";
                logger.error(errorMessage, e);
                internalListener.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        }
    }

    private void validateWorkflows(Template template) throws Exception {
        for (Workflow workflow : template.workflows().values()) {
            List<ProcessNode> sortedNodes = workflowProcessSorter.sortProcessNodes(workflow, null, Collections.emptyMap());
            workflowProcessSorter.validate(sortedNodes, pluginsService);
        }
    }
}
