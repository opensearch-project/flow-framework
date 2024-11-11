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
import org.opensearch.flowframework.model.Template;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.REPROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.UPDATE_WORKFLOW_FIELDS;

/**
 * Transport Request to create, provision, and deprovision a workflow
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
    private String[] validation;

    /**
     * Provision flag
     */
    private boolean provision;

    /**
     * Reprovision flag
     */
    private boolean reprovision;

    /**
     * Update Fields flag
     */
    private boolean updateFields;

    /**
     * Params map
     */
    private Map<String, String> params;

    /**
     * Instantiates a new WorkflowRequest, set validation to all, no provisioning
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     */
    public WorkflowRequest(@Nullable String workflowId, @Nullable Template template) {
        this(workflowId, template, new String[] { "all" }, false, Collections.emptyMap(), false);
    }

    /**
     * Instantiates a new WorkflowRequest with params map, set validation to all, provisioning to true
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     * @param params The parameters from the REST path
     */
    public WorkflowRequest(@Nullable String workflowId, @Nullable Template template, Map<String, String> params) {
        this(workflowId, template, new String[] { "all" }, true, params, false);
    }

    /**
     * Instantiates a new WorkflowRequest
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     * @param validation flag to indicate if validation is necessary
     * @param provisionOrUpdate provision or updateFields flag. Only one may be true, the presence of update_fields key in map indicates if updating fields, otherwise true means it's provisioning.
     * @param params map of REST path params. If provisionOrUpdate is false, must be an empty map. If update_fields key is present, must be only key.
     * @param reprovision flag to indicate if request is to reprovision
     */
    public WorkflowRequest(
        @Nullable String workflowId,
        @Nullable Template template,
        String[] validation,
        boolean provisionOrUpdate,
        Map<String, String> params,
        boolean reprovision
    ) {
        this.workflowId = workflowId;
        this.template = template;
        this.validation = validation;
        this.provision = provisionOrUpdate && !params.containsKey(UPDATE_WORKFLOW_FIELDS);
        this.updateFields = !provision && Boolean.parseBoolean(params.get(UPDATE_WORKFLOW_FIELDS));
        if (!this.provision && params.keySet().stream().anyMatch(k -> !UPDATE_WORKFLOW_FIELDS.equals(k))) {
            throw new IllegalArgumentException("Params may only be included when provisioning.");
        }
        this.params = this.updateFields ? Collections.emptyMap() : params;
        this.reprovision = reprovision;
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
        this.validation = in.readStringArray();
        boolean provisionOrUpdateOrReprovision = in.readBoolean();
        this.params = provisionOrUpdateOrReprovision
            ? in.readMap(StreamInput::readString, StreamInput::readString)
            : Collections.emptyMap();
        this.provision = provisionOrUpdateOrReprovision
            && !params.containsKey(UPDATE_WORKFLOW_FIELDS)
            && !params.containsKey(REPROVISION_WORKFLOW);
        this.updateFields = !provision && Boolean.parseBoolean(params.get(UPDATE_WORKFLOW_FIELDS));
        if (this.updateFields) {
            this.params = Collections.emptyMap();
        }
        this.reprovision = !provision && Boolean.parseBoolean(params.get(REPROVISION_WORKFLOW));
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
     * Gets the validation flag
     * @return the validation boolean
     */
    public String[] getValidation() {
        return this.validation;
    }

    /**
     * Gets the provision flag
     * @return the provision boolean
     */
    public boolean isProvision() {
        return this.provision;
    }

    /**
     * Gets the update fields flag
     * @return the update fields boolean
     */
    public boolean isUpdateFields() {
        return this.updateFields;
    }

    /**
     * Gets the params map
     * @return the params map
     */
    public Map<String, String> getParams() {
        return Map.copyOf(this.params);
    }

    /**
     * Gets the reprovision flag
     * @return the reprovision boolean
     */
    public boolean isReprovision() {
        return this.reprovision;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(workflowId);
        out.writeOptionalString(template == null ? null : template.toJson());
        out.writeStringArray(validation);
        out.writeBoolean(provision || updateFields || reprovision);
        if (provision) {
            out.writeMap(params, StreamOutput::writeString, StreamOutput::writeString);
        } else if (updateFields) {
            out.writeMap(Map.of(UPDATE_WORKFLOW_FIELDS, "true"), StreamOutput::writeString, StreamOutput::writeString);
        } else if (reprovision) {
            out.writeMap(Map.of(REPROVISION_WORKFLOW, "true"), StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
