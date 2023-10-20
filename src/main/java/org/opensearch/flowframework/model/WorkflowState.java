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
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * The WorkflowState is used to store all additional information regarding a workflow that isn't part of the
 * global context.
 */
public class WorkflowState implements ToXContentObject {
    public static final String WORKFLOW_ID_FIELD = "workflow_id";
    public static final String ERROR_FIELD = "error";
    public static final String STATE_FIELD = "state";
    public static final String PROVISIONING_PROGRESS_FIELD = "provisioning_progress";
    public static final String PROVISION_START_TIME_FIELD = "provision_start_time";
    public static final String PROVISION_END_TIME_FIELD = "provision_end_time";
    public static final String USER_FIELD = "user";
    public static final String UI_METADATA_FIELD = "ui_metadata";

    private String workflowId;
    private String error;
    private String state;
    private String provisioningProgress;
    private Instant provisionStartTime;
    private Instant provisionEndTime;
    private User user;
    private Map<String, Object> uiMetadata;

    /**
     * Instantiate the object representing the workflow state
     *
     * @param workflowId The workflow ID representing the given workflow
     * @param error
     * @param state
     * @param provisioningProgress
     * @param provisionStartTime
     * @param provisionEndTime
     * @param user
     * @param uiMetadata
     */
    public WorkflowState(
        String workflowId,
        String error,
        String state,
        String provisioningProgress,
        Instant provisionStartTime,
        Instant provisionEndTime,
        User user,
        Map<String, Object> uiMetadata
    ) {
        this.workflowId = workflowId;
        this.error = error;
        this.state = state;
        this.provisioningProgress = provisioningProgress;
        this.provisionStartTime = provisionStartTime;
        this.provisionEndTime = provisionEndTime;
        this.user = user;
        this.uiMetadata = uiMetadata;
    }

    private WorkflowState() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String workflowId = null;
        private String error = null;
        private String state = null;
        private String provisioningProgress = null;
        private Instant provisionStartTime = null;
        private Instant provisionEndTime = null;
        private User user = null;
        private Map<String, Object> uiMetadata = null;

        public Builder() {}

        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public Builder error(String error) {
            this.error = error;
            return this;
        }

        public Builder state(String state) {
            this.state = state;
            return this;
        }

        public Builder provisioningProgress(String provisioningProgress) {
            this.provisioningProgress = provisioningProgress;
            return this;
        }

        public Builder provisionStartTime(Instant provisionStartTime) {
            this.provisionStartTime = provisionStartTime;
            return this;
        }

        public Builder provisionEndTime(Instant provisionEndTime) {
            this.provisionEndTime = provisionEndTime;
            return this;
        }

        public Builder user(User user) {
            this.user = user;
            return this;
        }

        public Builder uiMetadata(Map<String, Object> uiMetadata) {
            this.uiMetadata = uiMetadata;
            return this;
        }

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
        return xContentBuilder.endObject();
    }

    // TODO: might need to add another parse that takes in a workflow ID.
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
                    provisionStartTime = ParseUtils.toInstant(parser);
                    break;
                case PROVISION_END_TIME_FIELD:
                    provisionEndTime = ParseUtils.toInstant(parser);
                    break;
                case USER_FIELD:
                    user = User.parse(parser);
                    break;
                case UI_METADATA_FIELD:
                    uiMetadata = parser.map();
                    break;
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
            .build();
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public String getError() {
        return workflowId;
    }

    public String getState() {
        return state;
    }

    public String getProvisioningProgress() {
        return provisioningProgress;
    }

    public Instant getProvisionStartTime() {
        return provisionStartTime;
    }

    public Instant getProvisionEndTime() {
        return provisionEndTime;
    }

    public User getUser() {
        return user;
    }

    public Map<String, Object> getUiMetadata() {
        return uiMetadata;
    }
}
