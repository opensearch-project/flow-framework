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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.flowframework.model.Template;

import java.io.IOException;

/**
 * Transport request to reprovision a workflow
 */
public class ReprovisionWorkflowRequest extends ActionRequest {

    /**
     * The workflow Id
     */
    private String workflowId;
    /**
     * The original template
     */
    private Template originalTemplate;
    /**
     * The updated template
     */
    private Template updatedTemplate;

    /**
     * Instantiates a new ReprovisionWorkflowRequest
     * @param workflowId the workflow ID
     * @param originalTemplate the original Template
     * @param updatedTemplate the updated Template
     */
    public ReprovisionWorkflowRequest(String workflowId, Template originalTemplate, Template updatedTemplate) {
        this.workflowId = workflowId;
        this.originalTemplate = originalTemplate;
        this.updatedTemplate = updatedTemplate;
    }

    /**
     * Instantiates a new ReprovisionWorkflow request
     * @param in The input stream to read from
     * @throws IOException If the stream cannot be read properly
     */
    public ReprovisionWorkflowRequest(StreamInput in) throws IOException {
        super(in);
        this.workflowId = in.readString();
        this.originalTemplate = Template.parse(in.readString());
        this.updatedTemplate = Template.parse(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(workflowId);
        out.writeString(originalTemplate.toJson());
        out.writeString(updatedTemplate.toJson());
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * Gets the workflow Id of the request
     * @return the workflow Id
     */
    public String getWorkflowId() {
        return this.workflowId;
    }

    /**
     * Gets the original template of the request
     * @return the original template
     */
    public Template getOriginalTemplate() {
        return this.originalTemplate;
    }

    /**
     * Gets the updated template of the request
     * @return the updated template
     */
    public Template getUpdatedTemplate() {
        return this.updatedTemplate;
    }

}
