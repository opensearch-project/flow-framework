/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport Request to get a workflow state
 */
public class GetWorkflowStateRequest extends ActionRequest {

    /**
     * The documentId of the workflow entry within the Global Context index
     */
    @Nullable
    private String workflowId;

    /**
     * The all parameter on the get request
     */
    private boolean all;

    /**
     * Instantiates a new GetWorkflowRequest
     * @param workflowId the documentId of the workflow
     * @param all whether the get request is looking for all fields in status
     */
    public GetWorkflowStateRequest(@Nullable String workflowId, boolean all) {
        this.workflowId = workflowId;
        this.all = all;
    }

    /**
     * Instantiates a new GetWorkflowRequest request
     * @param in The input stream to read from
     * @throws IOException If the stream cannot be read properly
     */
    public GetWorkflowStateRequest(StreamInput in) throws IOException {
        super(in);
        this.workflowId = in.readString();
        this.all = in.readBoolean();
    }

    /**
     * Gets the workflow Id of the request
     * @return the workflow Id
     */
    @Nullable
    public String getWorkflowId() {
        return this.workflowId;
    }

    /**
     * Gets the value of the all parameter
     * @return whether the all parameter was present or not in request
     */
    public boolean getAll() {
        return this.all;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(workflowId);
        out.writeBoolean(all);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
