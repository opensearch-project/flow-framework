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
public class GetWorkflowStateResponse extends ActionResponse implements ToXContentObject {

    /** The workflow state */
    private final WorkflowState workflowState;
    /** Flag to indicate if the entire state should be returned */
    private final boolean allStatus;

    /**
     * Instantiates a new GetWorkflowStateResponse from an input stream
     * @param in the input stream to read from
     * @throws IOException if the workflowId cannot be read from the input stream
     */
    public GetWorkflowStateResponse(StreamInput in) throws IOException {
        super(in);
        workflowState = new WorkflowState(in);
        allStatus = in.readBoolean();
    }

    /**
     * Instatiates a new GetWorkflowStateResponse from an input stream
     * @param workflowState the workflow state object
     * @param allStatus whether to return all fields in state index
     */
    public GetWorkflowStateResponse(WorkflowState workflowState, boolean allStatus) {
        this.allStatus = allStatus;
        if (allStatus) {
            this.workflowState = workflowState;
        } else {
            this.workflowState = WorkflowState.builder()
                .workflowId(workflowState.getWorkflowId())
                .error(workflowState.getError())
                .state(workflowState.getState())
                .resourcesCreated(workflowState.resourcesCreated())
                .build();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        workflowState.writeTo(out);
        out.writeBoolean(allStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return workflowState.toXContent(xContentBuilder, params);
    }

    /**
     * Gets the workflow state.
     * @return the workflow state
     */
    public WorkflowState getWorkflowState() {
        return workflowState;
    }

    /**
     * Gets whether to return the entire state.
     * @return true if the entire state should be returned
     */
    public boolean isAllStatus() {
        return allStatus;
    }
}
