package org.opensearch.flowframework.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class GetWorkflowStepTransportAction extends HandledTransportAction<WorkflowRequest, GetWorkflowStepResponse> {


    private final Logger logger = LogManager.getLogger(GetWorkflowStepTransportAction.class);
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private final Client client;

    /**
     * Instantiates a new GetWorkflowStepTransportAction instance
     * @param transportService the transport service
     * @param actionFilters action filters
     * @param flowFrameworkIndicesHandler The Flow Framework indices handler
     * @param client the OpenSearch Client
     */
    @Inject
    public GetWorkflowStepTransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
            Client client
    ) {
        super(GetWorkflowStepAction.NAME, transportService, actionFilters, WorkflowRequest::new);
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, WorkflowRequest request, ActionListener<GetWorkflowStepResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            listener.onResponse(new GetWorkflowStepResponse(WorkflowValidator.parse("mappings/workflow-steps.json")));
        } catch (Exception e) {
            logger.error("Failed to retrieve workflow step json.", e);
            listener.onFailure(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
        }
    }
}
