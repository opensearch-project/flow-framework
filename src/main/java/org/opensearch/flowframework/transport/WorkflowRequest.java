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
     * Params map
     */
    private Map<String, String> params;

    /**
     * use case flag
     */
    private String useCase;

    /**
     * Deafult params map from use case
     */
    private Map<String, String> defaultParams;

    /**
     * Instantiates a new WorkflowRequest, set validation to all, no provisioning
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     */
    public WorkflowRequest(@Nullable String workflowId, @Nullable Template template) {
        this(workflowId, template, new String[] { "all" }, false, Collections.emptyMap(), null, Collections.emptyMap());
    }

    /**
     * Instantiates a new WorkflowRequest with params map, set validation to all, provisioning to true
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     * @param params The parameters from the REST path
     */
    public WorkflowRequest(@Nullable String workflowId, @Nullable Template template, Map<String, String> params) {
        this(workflowId, template, new String[] { "all" }, true, params, null, Collections.emptyMap());
    }

    /**
     * Instantiates a new WorkflowRequest with params map, set validation to all, provisioning to true
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     * @param useCase the default use case give by user
     * @param defaultParams The parameters from the REST body when a use case is given
     */
    public WorkflowRequest(@Nullable String workflowId, @Nullable Template template, String useCase, Map<String, String> defaultParams) {
        this(workflowId, template, new String[] { "all" }, false, Collections.emptyMap(), useCase, defaultParams);
    }

    /**
     * Instantiates a new WorkflowRequest
     * @param workflowId the documentId of the workflow
     * @param template the use case template which describes the workflow
     * @param validation flag to indicate if validation is necessary
     * @param provision flag to indicate if provision is necessary
     * @param params map of REST path params. If provision is false, must be an empty map.
     * @param useCase default use case given
     * @param defaultParams the params to be used in the substitution based on the default use case.
     */
    public WorkflowRequest(
        @Nullable String workflowId,
        @Nullable Template template,
        String[] validation,
        boolean provision,
        Map<String, String> params,
        String useCase,
        Map<String, String> defaultParams
    ) {
        this.workflowId = workflowId;
        this.template = template;
        this.validation = validation;
        this.provision = provision;
        if (!provision && !params.isEmpty()) {
            throw new IllegalArgumentException("Params may only be included when provisioning.");
        }
        this.params = params;
        this.useCase = useCase;
        this.defaultParams = defaultParams;
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
        this.provision = in.readBoolean();
        this.params = this.provision ? in.readMap(StreamInput::readString, StreamInput::readString) : Collections.emptyMap();
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
     * Gets the params map
     * @return the params map
     */
    public Map<String, String> getParams() {
        return Map.copyOf(this.params);
    }

    /**
     * Gets the use case
     * @return the use case
     */
    public String getUseCase() {
        return this.useCase;
    }

    /**
     * Gets the params map
     * @return the params map
     */
    public Map<String, String> getDefaultParams() {
        return Map.copyOf(this.defaultParams);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(workflowId);
        out.writeOptionalString(template == null ? null : template.toJson());
        out.writeStringArray(validation);
        out.writeBoolean(provision);
        if (provision) {
            out.writeMap(params, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
