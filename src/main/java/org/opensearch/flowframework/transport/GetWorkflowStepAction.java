package org.opensearch.flowframework.transport;

import org.opensearch.action.ActionType;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.flowframework.model.WorkflowStepValidator;

import static org.opensearch.flowframework.common.CommonValue.TRANSPORT_ACTION_NAME_PREFIX;

public class GetWorkflowStepAction extends ActionType<GetWorkflowStepResponse> {

    /** The name of this action */
    public static final String NAME = TRANSPORT_ACTION_NAME_PREFIX + "workflow_step/get";
    /** An instance of this action */
    public static final GetWorkflowStepAction INSTANCE = new GetWorkflowStepAction();

    public GetWorkflowStepAction() {
        super(NAME, GetWorkflowStepResponse::new);
    }
}
