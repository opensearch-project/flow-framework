/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.model.WorkflowState;

import java.io.IOException;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

/**
 * Transport Response from creating or provisioning a workflow
 */
public class WorkflowResponse extends ActionResponse implements ToXContentObject {

    /**
     * The documentId of the workflow entry within the Global Context index
     */
    private String workflowId;
    /** The workflow state */
    private WorkflowState workflowState;

    /**
     * Instantiates a new WorkflowResponse from params
     * @param workflowId the documentId of the indexed use case template
     */
    public WorkflowResponse(String workflowId) {
        this.workflowId = workflowId;
    }

    /**
     * Instatiates a new WorkflowResponse from an input stream
     * @param in the input stream to read from
     * @throws IOException if the workflowId cannot be read from the input stream
     */
    public WorkflowResponse(StreamInput in) throws IOException {
        super(in);
        this.workflowId = in.readString();
        if (in.getVersion().onOrAfter(Version.V_2_19_0)) {
            this.workflowState = in.readOptionalWriteable(WorkflowState::new);
        }

    }

    /**
     * Gets the workflowId of this repsonse
     * @return the workflowId
     */
    public String getWorkflowId() {
        return this.workflowId;
    }

    /**
     * Gets the workflowState of this repsonse
     * @return the workflowState
     */
    @Nullable
    public WorkflowState getWorkflowState() {
        return this.workflowState;
    }

    /**
     * Constructs a new WorkflowResponse object with the specified workflowId and workflowState.
     * The WorkflowResponse is typically returned as part of a `wait_for_completion` request,
     * indicating the final state of a workflow after execution.
     * @param workflowId The unique identifier for the workflow.
     * @param workflowState The current state of the workflow, including status, errors (if any),
     *                      and resources created as part of the workflow execution.
     */
    public WorkflowResponse(String workflowId, WorkflowState workflowState) {
        this.workflowId = workflowId;
        this.workflowState = WorkflowState.builder()
            .workflowId(workflowId)
            .error(workflowState.getError())
            .state(workflowState.getState())
            .resourcesCreated(workflowState.resourcesCreated())
            .build();

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(workflowId);
        if (out.getVersion().onOrAfter(Version.V_2_19_0)) {
            out.writeOptionalWriteable(workflowState);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (workflowState != null) {
            return workflowState.toXContent(builder, params);
        } else {
            return builder.startObject().field(WORKFLOW_ID, this.workflowId).endObject();
        }
    }

}
