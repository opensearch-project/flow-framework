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
import org.opensearch.action.DocRequest;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.flowframework.common.CommonValue;

import java.io.IOException;

/**
 * Transport Request to get a workflow status
 */
public class GetWorkflowStateRequest extends ActionRequest implements DocRequest {

    /**
     * The documentId of the workflow entry within the Global Context index
     */
    @Nullable
    private String workflowId;

    /**
     * The all parameter on the get request
     */
    private boolean all;

    private String tenantId;

    /**
     * Instantiates a new GetWorkflowStateRequest
     * @param workflowId the documentId of the workflow
     * @param all whether the get request is looking for all fields in status
     * @param tenantId the tenant id
     */
    public GetWorkflowStateRequest(@Nullable String workflowId, boolean all, String tenantId) {
        this.workflowId = workflowId;
        this.all = all;
        this.tenantId = tenantId;
    }

    /**
     * Instantiates a new GetWorkflowStateRequest request
     * @param in The input stream to read from
     * @throws IOException If the stream cannot be read properly
     */
    public GetWorkflowStateRequest(StreamInput in) throws IOException {
        super(in);
        this.workflowId = in.readString();
        this.all = in.readBoolean();
        if (in.getVersion().onOrAfter(CommonValue.VERSION_2_19_0)) {
            this.tenantId = in.readOptionalString();
        }
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

    /**
     * Gets the tenant Id
     * @return the tenant id
     */
    public String getTenantId() {
        return this.tenantId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(workflowId);
        out.writeBoolean(all);
        if (out.getVersion().onOrAfter(CommonValue.VERSION_2_19_0)) {
            out.writeOptionalString(tenantId);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String index() {
        return CommonValue.WORKFLOW_STATE_INDEX;
    }

    @Override
    public String id() {
        return workflowId;
    }
}
