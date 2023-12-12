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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.flowframework.model.Template;

import java.io.IOException;

/**
 * Transport Request to create and provision a workflow
 */
public class WorkflowRequest extends ActionRequest {

    /**
     * The documentId of the workflow entry within the Global Context index
     */
    @Nullable
    private String workflowId;
    /**
     * The use case template to index
     */
    @Nullable
    private Template template;
    /**
     * Validation flag
     */
    private boolean dryRun;

    /**
     * Provision flag
     */
    private boolean provision;

    /**
     * Timeout for request
     */
    private TimeValue requestTimeout;

    /**
     * Max workflows
     */
    private Integer maxWorkflows;

    /**
     * Instantiates a new WorkflowRequest, defaults dry run to false and set requestTimeout and maxWorkflows to null
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     */
    public WorkflowRequest(@Nullable String workflowId, @Nullable Template template) {
        this(workflowId, template, false, null, null);
    }

    /**
     * Instantiates a new WorkflowRequest and defaults dry run to false
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     * @param requestTimeout timeout of the request
     * @param maxWorkflows max number of workflows
     */
    public WorkflowRequest(
        @Nullable String workflowId,
        @Nullable Template template,
        @Nullable TimeValue requestTimeout,
        @Nullable Integer maxWorkflows
    ) {
        this(workflowId, template, false, requestTimeout, maxWorkflows);
    }

    /**
     * Instantiates a new WorkflowRequest
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     * @param provision flag to indicate if provision is necessary
     * @param requestTimeout timeout of the request
     * @param maxWorkflows max number of workflows
     */
    public WorkflowRequest(
        @Nullable String workflowId,
        @Nullable Template template,
        boolean provision,
        @Nullable TimeValue requestTimeout,
        @Nullable Integer maxWorkflows
    ) {
        this.workflowId = workflowId;
        this.template = template;
        this.provision = provision;
        this.requestTimeout = requestTimeout;
        this.maxWorkflows = maxWorkflows;
    }

    /**
     * Instantiates a new Workflow request
     * @param in The input stream to read from
     * @throws IOException If the stream cannot be read properly
     */
    public WorkflowRequest(StreamInput in) throws IOException {
        super(in);
        this.workflowId = in.readOptionalString();
        String templateJson = in.readOptionalString();
        this.template = templateJson == null ? null : Template.parse(templateJson);
        this.provision = in.readBoolean();
        this.requestTimeout = in.readOptionalTimeValue();
        this.maxWorkflows = in.readOptionalInt();
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
     * Gets the use case template of the request
     * @return the use case template
     */
    @Nullable
    public Template getTemplate() {
        return this.template;
    }

    /**
     * Gets the provision flag
     * @return the provision boolean
     */
    public boolean isProvision() {
        return this.provision;
    }

    /**
     * Gets the timeout of the request
     * @return the requestTimeout
     */
    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Gets the max workflows
     * @return the maxWorkflows
     */
    public Integer getMaxWorkflows() {
        return maxWorkflows;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(workflowId);
        out.writeOptionalString(template == null ? null : template.toJson());
        out.writeBoolean(dryRun);
        out.writeOptionalTimeValue(requestTimeout);
        out.writeOptionalInt(maxWorkflows);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

}
