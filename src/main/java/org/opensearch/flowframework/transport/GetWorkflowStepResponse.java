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
import org.opensearch.flowframework.model.WorkflowValidator;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;

import java.io.IOException;

/**
 * Transport Response from getting workflow step
 */
public class GetWorkflowStepResponse extends ActionResponse implements ToXContentObject {

    private WorkflowValidator workflowValidator;
    private WorkflowStepFactory workflowStepFactory;

    /**
     * Instantiates a new GetWorkflowStepResponse from an input stream
     * @param in the input stream to read from
     * @throws IOException if the workflow json cannot be read from the input stream
     */
    public GetWorkflowStepResponse(StreamInput in) throws IOException {
        super(in);
        this.workflowValidator = this.workflowStepFactory.getWorkflowValidator();
    }

    /**
     * Instantiates a new GetWorkflowStepResponse
     * @param workflowValidator the workflow validator
     */
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
