package org.opensearch.flowframework.transport;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.model.WorkflowStepValidator;
import org.opensearch.flowframework.model.WorkflowValidator;

import java.io.IOException;

public class GetWorkflowStepResponse extends ActionResponse implements ToXContentObject {

    private WorkflowValidator workflowValidator;

    public GetWorkflowStepResponse(StreamInput in) throws IOException {
        super(in);
        this.workflowValidator = WorkflowValidator.parse(in.readString());
    }

    public GetWorkflowStepResponse(WorkflowValidator workflowValidator) {
        this.workflowValidator = workflowValidator;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(workflowValidator.toJson());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return this.workflowValidator.toXContent(xContentBuilder, params);
    }
}
