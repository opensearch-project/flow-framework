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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    private final Settings settings;
    private final PluginsService pluginsService;

    /**
     * Intantiates a new CreateWorkflowTransportAction
     * @param transportService the TransportService
     * @param actionFilters action filters
     * @param workflowProcessSorter the workflow process sorter
     * @param flowFrameworkIndicesHandler The handler for the global context index
     * @param settings Environment settings
     * @param client The client used to make the request to OS
     * @param pluginsService The plugin service
     */
    @Inject
    public CreateWorkflowTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        WorkflowProcessSorter workflowProcessSorter,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        Settings settings,
        Client client,
        PluginsService pluginsService
    ) {
        super(CreateWorkflowAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.workflowProcessSorter = workflowProcessSorter;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.settings = settings;
        this.client = client;
        this.pluginsService = pluginsService;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<WorkflowResponse> listener) {

        User user = getUserContext(client);
        Template templateWithUser = new Template(
            request.getTemplate().name(),
            request.getTemplate().description(),
            request.getTemplate().useCase(),
            request.getTemplate().templateVersion(),
            request.getTemplate().compatibilityVersion(),
            request.getTemplate().workflows(),
            request.getTemplate().getUiMetadata(),
            user
        );

        String[] validateAll = { "all" };
        if (Arrays.equals(request.getValidation(), validateAll)) {
            try {
                validateWorkflows(templateWithUser);
            } catch (Exception e) {
                logger.error("Workflow validation failed for template: {}", templateWithUser.name());
                listener.onFailure(
                    e instanceof FlowFrameworkException ? e : new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e))
                );
                return;
            }
        }

        if (request.getWorkflowId() == null) {
            // Throttle incoming requests
            checkMaxWorkflows(request.getRequestTimeout(), request.getMaxWorkflows(), ActionListener.wrap(max -> {
                if (!max) {
                    String errorMessage = "Maximum workflows limit reached " + request.getMaxWorkflows();
                    logger.error(errorMessage);
                    FlowFrameworkException ffe = new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
                    listener.onFailure(ffe);
                    return;
                } else {
                    // Initialize config index and create new global context and state index entries
                    flowFrameworkIndicesHandler.initializeConfigIndex(ActionListener.wrap(isInitialized -> {
                        if (!isInitialized) {
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
                                            logger.info("create state workflow doc");
                                            if (request.isProvision()) {
                                                logger.info("provision parameter");
                                                WorkflowRequest workflowRequest = new WorkflowRequest(globalContextResponse.getId(), null);
                                                client.execute(
                                                    ProvisionWorkflowAction.INSTANCE,
                                                    workflowRequest,
                                                    ActionListener.wrap(provisionResponse -> {
                                                        listener.onResponse(new WorkflowResponse(provisionResponse.getWorkflowId()));
                                                    }, exception -> {
                                                        if (exception instanceof FlowFrameworkException) {
                                                            listener.onFailure(exception);
                                                        } else {
                                                            listener.onFailure(
                                                                new FlowFrameworkException(exception.getMessage(), RestStatus.BAD_REQUEST)
                                                            );
                                                        }
                                                        logger.error("Failed to send back provision workflow exception", exception);
                                                    })
                                                );
                                            } else {
                                                listener.onResponse(new WorkflowResponse(globalContextResponse.getId()));
                                            }
                                        }, exception -> {
                                            logger.error("Failed to save workflow state : {}", exception.getMessage());
                                            if (exception instanceof FlowFrameworkException) {
                                                listener.onFailure(exception);
                                            } else {
                                                listener.onFailure(
                                                    new FlowFrameworkException(exception.getMessage(), RestStatus.BAD_REQUEST)
                                                );
                                            }
                                        })
                                    );
                                }, exception -> {
                                    logger.error("Failed to save use case template : {}", exception.getMessage());
                                    if (exception instanceof FlowFrameworkException) {
                                        listener.onFailure(exception);
                                    } else {
                                        listener.onFailure(
                                            new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                                        );
                                    }

                                })
                            );
                        }
                    }, exception -> {
                        logger.error("Failed to initialize config index : {}", exception.getMessage());
                        if (exception instanceof FlowFrameworkException) {
                            listener.onFailure(exception);
                        } else {
                            listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
                        }

                    }));
                }
            }, e -> {
                logger.error("Failed to updated use case template {} : {}", request.getWorkflowId(), e.getMessage());
                if (e instanceof FlowFrameworkException) {
                    listener.onFailure(e);
                } else {
                    listener.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }
            }));
        } else {
            // Update existing entry, full document replacement
            flowFrameworkIndicesHandler.updateTemplateInGlobalContext(
                request.getWorkflowId(),
                request.getTemplate(),
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
                            logger.error("Failed to update workflow state : {}", exception.getMessage());
                            if (exception instanceof FlowFrameworkException) {
                                listener.onFailure(exception);
                            } else {
                                listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
                            }
                        })
                    );
                }, exception -> {
                    logger.error("Failed to updated use case template {} : {}", request.getWorkflowId(), exception.getMessage());
                    if (exception instanceof FlowFrameworkException) {
                        listener.onFailure(exception);
                    } else {
                        listener.onFailure(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
                    }

                })
            );
        }
    }

    /**
     * Checks if the max workflows limit has been reachesd
     *  @param requestTimeOut request time out
     *  @param maxWorkflow max workflows
     *  @param internalListener listener for search request
     */
    protected void checkMaxWorkflows(TimeValue requestTimeOut, Integer maxWorkflow, ActionListener<Boolean> internalListener) {
        if (!flowFrameworkIndicesHandler.doesIndexExist(CommonValue.GLOBAL_CONTEXT_INDEX)) {
            internalListener.onResponse(true);
        } else {
            QueryBuilder query = QueryBuilders.matchAllQuery();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(0).timeout(requestTimeOut);

            SearchRequest searchRequest = new SearchRequest(CommonValue.GLOBAL_CONTEXT_INDEX).source(searchSourceBuilder);

            client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                internalListener.onResponse(searchResponse.getHits().getTotalHits().value < maxWorkflow);
            }, exception -> {
                logger.error("Unable to fetch the workflows", exception);
                internalListener.onFailure(new FlowFrameworkException("Unable to fetch the workflows", RestStatus.BAD_REQUEST));
            }));
        }
    }

    private void validateWorkflows(Template template) throws Exception {
        for (Workflow workflow : template.workflows().values()) {
            List<ProcessNode> sortedNodes = workflowProcessSorter.sortProcessNodes(workflow, null);
            workflowProcessSorter.validate(sortedNodes, pluginsService);
        }
    }
}
