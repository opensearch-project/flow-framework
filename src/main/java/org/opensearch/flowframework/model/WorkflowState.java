/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.ParseUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.ERROR_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISIONING_PROGRESS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_END_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_START_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.RESOURCES_CREATED_FIELD;
import static org.opensearch.flowframework.common.CommonValue.STATE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.USER_FIELD;
import static org.opensearch.flowframework.common.CommonValue.USER_OUTPUTS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID_FIELD;
import static org.opensearch.flowframework.util.ParseUtils.parseStringToStringMap;

/**
 * The WorkflowState is used to store all additional information regarding a workflow that isn't part of the
 * global context.
 */
public class WorkflowState implements ToXContentObject, Writeable {
    private String workflowId;
    private String error;
    private String state;
    // TODO: Transition the provisioning progress from a string to detailed array of objects
    private String provisioningProgress;
    private Instant provisionStartTime;
    private Instant provisionEndTime;
    private User user;
    private Map<String, Object> userOutputs;
    private List<ResourceCreated> resourcesCreated;

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
        Map<String, Object> userOutputs,
        List<ResourceCreated> resourcesCreated
    ) {
        this.workflowId = workflowId;
        this.error = error;
        this.state = state;
        this.provisioningProgress = provisioningProgress;
        this.provisionStartTime = provisionStartTime;
        this.provisionEndTime = provisionEndTime;
        this.user = user;
        this.userOutputs = Map.copyOf(userOutputs);
        this.resourcesCreated = List.copyOf(resourcesCreated);
    }

    private WorkflowState() {}

    /**
     * Instatiates a new WorkflowState from an input stream
     * @param input the input stream to read from
     * @throws IOException if the workflowId cannot be read from the input stream
     */
    public WorkflowState(StreamInput input) throws IOException {
        this.workflowId = input.readString();
        this.error = input.readOptionalString();
        this.state = input.readOptionalString();
        this.provisioningProgress = input.readOptionalString();
        this.provisionStartTime = input.readOptionalInstant();
        this.provisionEndTime = input.readOptionalInstant();
        // TODO: fix error: cannot access Response issue when integrating with access control
        // this.user = input.readBoolean() ? new User(input) : null;
        this.userOutputs = input.readBoolean() ? input.readMap() : null;

        int resourceCount = input.readVInt();
        this.resourcesCreated = new ArrayList<>(resourceCount);
        for (int r = 0; r < resourceCount; r++) {
            resourcesCreated.add(new ResourceCreated(input));
        }
    }

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
        private Map<String, Object> userOutputs = null;
        private List<ResourceCreated> resourcesCreated = null;

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
        public Builder resourcesCreated(List<ResourceCreated> resourcesCreated) {
            this.resourcesCreated = resourcesCreated;
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
        if (userOutputs != null && !userOutputs.isEmpty()) {
            xContentBuilder.field(USER_OUTPUTS_FIELD, userOutputs);
        }
        if (resourcesCreated != null && !resourcesCreated.isEmpty()) {
            xContentBuilder.field(RESOURCES_CREATED_FIELD, resourcesCreated.toArray());
        }
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(workflowId);
        output.writeOptionalString(error);
        output.writeOptionalString(state);
        output.writeOptionalString(provisioningProgress);
        output.writeOptionalInstant(provisionStartTime);
        output.writeOptionalInstant(provisionEndTime);

        /*- TODO: fix error: cannot access Response issue when integrating with access control
        if (user != null) {
            output.writeBoolean(true);
            user.writeTo(output);
        } else {
            output.writeBoolean(false);
        }
        */

        if (userOutputs != null) {
            output.writeBoolean(true);
            output.writeMap(userOutputs);
        } else {
            output.writeBoolean(false);
        }

        output.writeVInt(resourcesCreated.size());
        for (ResourceCreated resource : resourcesCreated) {
            resource.writeTo(output);
        }
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
        Map<String, Object> userOutputs = new HashMap<>();
        List<ResourceCreated> resourcesCreated = new ArrayList<>();

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
                    try {
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            resourcesCreated.add(ResourceCreated.parse(parser));
                        }
                    } catch (Exception e) {
                        if (e instanceof ParsingException || e instanceof XContentParseException) {
                            throw new FlowFrameworkException("Error parsing newly created resources", RestStatus.INTERNAL_SERVER_ERROR);
                        }
                        throw e;
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
            .userOutputs(userOutputs)
            .resourcesCreated(resourcesCreated)
            .build();
    }

    /**
     * Parse a JSON workflow state
     * @param json A string containing a JSON representation of a workflow state
     * @return A {@link WorkflowState} represented by the JSON
     * @throws IOException on failure to parse
     */
    public static WorkflowState parse(String json) throws IOException {
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            json
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        return parse(parser);
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
        return error;
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
    public List<ResourceCreated> resourcesCreated() {
        return resourcesCreated;
    }

    @Override
    public String toString() {
        return "WorkflowState [workflowId="
            + workflowId
            + ", error="
            + error
            + ", state="
            + state
            + ", provisioningProgress="
            + provisioningProgress
            + ", userOutputs="
            + userOutputs
            + ", resourcesCreated="
            + resourcesCreated
            + "]";
    }
}
