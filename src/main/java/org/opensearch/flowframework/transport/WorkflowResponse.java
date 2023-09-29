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
import org.opensearch.flowframework.rest.RestCreateWorkflowAction;

import java.io.IOException;

/**
 * Transport Response from creating or provisioning a workflow
 */
public class WorkflowResponse extends ActionResponse implements ToXContentObject {

    /**
     * The documentId of the workflow entry within the Global Context index
     */
    private String workflowId;

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
    }

    /**
     * Gets the workflowId of this repsonse
     * @return the workflowId
     */
    public String getWorkflowId() {
        return this.workflowId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(workflowId);
    }

    // TODO : Replace WORKFLOW_ID after string is moved to common values class
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(RestCreateWorkflowAction.WORKFLOW_ID, this.workflowId).endObject();
    }

}
