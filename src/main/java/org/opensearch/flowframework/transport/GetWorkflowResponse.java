/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.model.WorkflowState;

import java.io.IOException;

/**
 * Transport Response from getting a workflow status
 */
public class GetWorkflowResponse extends ActionResponse implements ToXContentObject {

    public WorkflowState workflowState;
    public boolean allStatus;

    /**
     * Instantiates a new GetWorkflowResponse from an input stream
     * @param in the input stream to read from
     * @throws IOException if the workflowId cannot be read from the input stream
     */
    public GetWorkflowResponse(StreamInput in) throws IOException {
        super(in);
        workflowState = new WorkflowState(in);
        allStatus = false;
    }

    /**
     * Instatiates a new GetWorkflowResponse from an input stream
     * @param workflowState the workflow state object
     * @param allStatus whether to return all fields in state index
     */
    public GetWorkflowResponse(WorkflowState workflowState, boolean allStatus) {
        if (allStatus) {
            this.workflowState = workflowState;
        } else {
            this.workflowState = new WorkflowState.Builder().workflowId(workflowState.getWorkflowId())
                .error(workflowState.getError())
                .state(workflowState.getState())
                .resourcesCreated(workflowState.resourcesCreated())
                .build();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        workflowState.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return workflowState.toXContent(xContentBuilder, params);
    }
}
