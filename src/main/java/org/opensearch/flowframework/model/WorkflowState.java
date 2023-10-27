/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.util.ParseUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.util.ParseUtils.parseStringToStringMap;

/**
 * The WorkflowState is used to store all additional information regarding a workflow that isn't part of the
 * global context.
 */
public class WorkflowState implements ToXContentObject {
    /** The template field name for the associated workflowID **/
    public static final String WORKFLOW_ID_FIELD = "workflow_id";
    /** The template field name for the workflow error **/
    public static final String ERROR_FIELD = "error";
    /** The template field name for the workflow state **/
    public static final String STATE_FIELD = "state";
    /** The template field name for the workflow provisioning progress **/
    public static final String PROVISIONING_PROGRESS_FIELD = "provisioning_progress";
    /** The template field name for the workflow provisioning start time **/
    public static final String PROVISION_START_TIME_FIELD = "provision_start_time";
    /** The template field name for the workflow provisioning end time **/
    public static final String PROVISION_END_TIME_FIELD = "provision_end_time";
    /** The template field name for the user who created the workflow **/
    public static final String USER_FIELD = "user";
    /** The template field name for the workflow ui metadata **/
    public static final String UI_METADATA_FIELD = "ui_metadata";
    /** The template field name for template user outputs */
    public static final String USER_OUTPUTS_FIELD = "user_outputs";
    /** The template field name for template resources created */
    public static final String RESOURCES_CREATED_FIELD = "resources_created";

    private String workflowId;
    private String error;
    private String state;
    // TODO: Tranisiton the provisioning progress from a string to detailed array of objects
    private String provisioningProgress;
    private Instant provisionStartTime;
    private Instant provisionEndTime;
    private User user;
    private Map<String, Object> uiMetadata;
    private Map<String, Object> userOutputs;
    private Map<String, Object> resourcesCreated;

    /**
     * Instantiate the object representing the workflow state
     *
     * @param workflowId The workflow ID representing the given workflow
     * @param error The error message if there is one for the current workflow
     * @param state The state of the current workflow
     * @param provisioningProgress Indicates the provisioning progress
     * @param provisionStartTime Indicates the start time of the whole provisioning flow
     * @param provisionEndTime Indicates the end time of the whole provisioning flow
     * @param user The user extracted from the thread context from the request
     * @param uiMetadata The UI metadata related to the given workflow
     * @param userOutputs A map of essential API responses for backend to use and lookup.
     * @param resourcesCreated A map of all the resources created.
     */
    public WorkflowState(
        String workflowId,
        String error,
        String state,
        String provisioningProgress,
        Instant provisionStartTime,
        Instant provisionEndTime,
        User user,
        Map<String, Object> uiMetadata,
        Map<String, Object> userOutputs,
        Map<String, Object> resourcesCreated
    ) {
        this.workflowId = workflowId;
        this.error = error;
        this.state = state;
        this.provisioningProgress = provisioningProgress;
        this.provisionStartTime = provisionStartTime;
        this.provisionEndTime = provisionEndTime;
        this.user = user;
        this.uiMetadata = uiMetadata;
        this.userOutputs = Map.copyOf(userOutputs);
        this.resourcesCreated = Map.copyOf(resourcesCreated);
    }

    private WorkflowState() {}

    /**
     * Constructs a builder object for workflowState
     * @return Builder Object
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Class for constructing a Builder for WorkflowState
     */
    public static class Builder {
        private String workflowId = null;
        private String error = null;
        private String state = null;
        private String provisioningProgress = null;
        private Instant provisionStartTime = null;
        private Instant provisionEndTime = null;
        private User user = null;
        private Map<String, Object> uiMetadata = null;
        private Map<String, Object> userOutputs = null;
        private Map<String, Object> resourcesCreated = null;

        /**
         * Empty Constructor for the Builder object
         */
        public Builder() {}

        /**
         * Builder method for adding workflowID
         * @param workflowId workflowId
         * @return the Builder object
         */
        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        /**
         * Builder method for adding error
         * @param error error
         * @return the Builder object
         */
        public Builder error(String error) {
            this.error = error;
            return this;
        }

        /**
         * Builder method for adding state
         * @param state state
         * @return the Builder object
         */
        public Builder state(String state) {
            this.state = state;
            return this;
        }

        /**
         * Builder method for adding provisioningProgress
         * @param provisioningProgress provisioningProgress
         * @return the Builder object
         */
        public Builder provisioningProgress(String provisioningProgress) {
            this.provisioningProgress = provisioningProgress;
            return this;
        }

        /**
         * Builder method for adding provisionStartTime
         * @param provisionStartTime provisionStartTime
         * @return the Builder object
         */
        public Builder provisionStartTime(Instant provisionStartTime) {
            this.provisionStartTime = provisionStartTime;
            return this;
        }

        /**
         * Builder method for adding provisionEndTime
         * @param provisionEndTime provisionEndTime
         * @return the Builder object
         */
        public Builder provisionEndTime(Instant provisionEndTime) {
            this.provisionEndTime = provisionEndTime;
            return this;
        }

        /**
         * Builder method for adding user
         * @param user user
         * @return the Builder object
         */
        public Builder user(User user) {
            this.user = user;
            return this;
        }

        /**
         * Builder method for adding uiMetadata
         * @param uiMetadata uiMetadata
         * @return the Builder object
         */
        public Builder uiMetadata(Map<String, Object> uiMetadata) {
            this.uiMetadata = uiMetadata;
            return this;
        }

        /**
         * Builder method for adding userOutputs
         * @param userOutputs userOutputs
         * @return the Builder object
         */
        public Builder userOutputs(Map<String, Object> userOutputs) {
            this.userOutputs = userOutputs;
            return this;
        }

        /**
         * Builder method for adding resourcesCreated
         * @param resourcesCreated resourcesCreated
         * @return the Builder object
         */
        public Builder resourcesCreated(Map<String, Object> resourcesCreated) {
            this.userOutputs = resourcesCreated;
            return this;
        }

        /**
         * Allows building a workflowState
         * @return WorkflowState workflowState Object containing all needed fields
         */
        public WorkflowState build() {
            WorkflowState workflowState = new WorkflowState();
            workflowState.workflowId = this.workflowId;
            workflowState.error = this.error;
            workflowState.state = this.state;
            workflowState.provisioningProgress = this.provisioningProgress;
            workflowState.provisionStartTime = this.provisionStartTime;
            workflowState.provisionEndTime = this.provisionEndTime;
            workflowState.user = this.user;
            workflowState.uiMetadata = this.uiMetadata;
            workflowState.userOutputs = this.userOutputs;
            workflowState.resourcesCreated = this.resourcesCreated;
            return workflowState;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        if (workflowId != null) {
            xContentBuilder.field(WORKFLOW_ID_FIELD, workflowId);
        }
        if (error != null) {
            xContentBuilder.field(ERROR_FIELD, error);
        }
        if (state != null) {
            xContentBuilder.field(STATE_FIELD, state);
        }
        if (provisioningProgress != null) {
            xContentBuilder.field(PROVISIONING_PROGRESS_FIELD, provisioningProgress);
        }
        if (provisionStartTime != null) {
            xContentBuilder.field(PROVISION_START_TIME_FIELD, provisionStartTime.toEpochMilli());
        }
        if (provisionEndTime != null) {
            xContentBuilder.field(PROVISION_END_TIME_FIELD, provisionEndTime.toEpochMilli());
        }
        if (user != null) {
            xContentBuilder.field(USER_FIELD, user);
        }
        if (uiMetadata != null && !uiMetadata.isEmpty()) {
            xContentBuilder.field(UI_METADATA_FIELD, uiMetadata);
        }
        if (userOutputs != null && !userOutputs.isEmpty()) {
            xContentBuilder.field(USER_OUTPUTS_FIELD, userOutputs);
        }
        if (resourcesCreated != null && !resourcesCreated.isEmpty()) {
            xContentBuilder.field(RESOURCES_CREATED_FIELD, resourcesCreated);
        }
        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into a Template instance.
     *
     * @param parser json based content parser
     * @return an instance of the template
     * @throws IOException if content can't be parsed correctly
     */
    public static WorkflowState parse(XContentParser parser) throws IOException {
        String workflowId = null;
        String error = null;
        String state = null;
        String provisioningProgress = null;
        Instant provisionStartTime = null;
        Instant provisionEndTime = null;
        User user = null;
        Map<String, Object> uiMetadata = null;
        Map<String, Object> userOutputs = new HashMap<>();
        Map<String, Object> resourcesCreated = new HashMap<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case WORKFLOW_ID_FIELD:
                    workflowId = parser.text();
                    break;
                case ERROR_FIELD:
                    error = parser.text();
                    break;
                case STATE_FIELD:
                    state = parser.text();
                    break;
                case PROVISIONING_PROGRESS_FIELD:
                    provisioningProgress = parser.text();
                    break;
                case PROVISION_START_TIME_FIELD:
                    provisionStartTime = ParseUtils.parseInstant(parser);
                    break;
                case PROVISION_END_TIME_FIELD:
                    provisionEndTime = ParseUtils.parseInstant(parser);
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                case UI_METADATA_FIELD:
                    uiMetadata = parser.map();
                    break;
                case USER_OUTPUTS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String userOutputsFieldName = parser.currentName();
                        switch (parser.nextToken()) {
                            case VALUE_STRING:
                                userOutputs.put(userOutputsFieldName, parser.text());
                                break;
                            case START_OBJECT:
                                userOutputs.put(userOutputsFieldName, parseStringToStringMap(parser));
                                break;
                            default:
                                throw new IOException("Unable to parse field [" + userOutputsFieldName + "] in a user_outputs object.");
                        }
                    }
                    break;

                case RESOURCES_CREATED_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String resourcesCreatedField = parser.currentName();
                        switch (parser.nextToken()) {
                            case VALUE_STRING:
                                resourcesCreated.put(resourcesCreatedField, parser.text());
                                break;
                            case START_OBJECT:
                                resourcesCreated.put(resourcesCreatedField, parseStringToStringMap(parser));
                                break;
                            default:
                                throw new IOException(
                                    "Unable to parse field [" + resourcesCreatedField + "] in a resources_created object."
                                );
                        }
                    }
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a workflowState object.");
            }
        }
        return new Builder().workflowId(workflowId)
            .error(error)
            .state(state)
            .provisioningProgress(provisioningProgress)
            .provisionStartTime(provisionStartTime)
            .provisionEndTime(provisionEndTime)
            .user(user)
            .uiMetadata(uiMetadata)
            .userOutputs(userOutputs)
            .resourcesCreated(resourcesCreated)
            .build();
    }

    /**
     * The workflowID associated with this workflow-state
     * @return the workflowId
     */
    public String getWorkflowId() {
        return workflowId;
    }

    /**
     * The main error seen in the workflow state if there is one
     * @return the error
     */
    public String getError() {
        return workflowId;
    }

    /**
     * The state of the current workflow
     * @return the state
     */
    public String getState() {
        return state;
    }

    /**
     * The state of the current provisioning
     * @return the provisioningProgress
     */
    public String getProvisioningProgress() {
        return provisioningProgress;
    }

    /**
     * The start time for the whole provision flow
     * @return the provisionStartTime
     */
    public Instant getProvisionStartTime() {
        return provisionStartTime;
    }

    /**
     * The end time for the whole provision flow
     * @return the provisionEndTime
     */
    public Instant getProvisionEndTime() {
        return provisionEndTime;
    }

    /**
     * User that created and owns this workflow
     * @return the user
     */
    public User getUser() {
        return user;
    }

    /**
     * A map corresponding to the UI metadata
     * @return the userOutputs
     */
    public Map<String, Object> getUiMetadata() {
        return uiMetadata;
    }

    /**
     * A map of essential API responses
     * @return the userOutputs
     */
    public Map<String, Object> userOutputs() {
        return userOutputs;
    }

    /**
     * A map of all the resources created
     * @return the resources created
     */
    public Map<String, Object> resourcesCreated() {
        return resourcesCreated;
    }
}
