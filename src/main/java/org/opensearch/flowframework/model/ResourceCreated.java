/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.RESOURCE_ID_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STEP_NAME;

/**
 * This represents an object in the WorkflowState {@link WorkflowState}.
 */
// TODO: create an enum to add the resource name itself for each step example (create_connector_step -> connector)
public class ResourceCreated implements ToXContentObject, Writeable {

    private final String workflowStepName;
    private final String resourceId;

    /**
     * Create this resources created object with given resource name and ID.
     * @param workflowStepName The workflow step name associating to the step where it was created
     * @param resourceId The resources ID for relating to the created resource
     */
    public ResourceCreated(String workflowStepName, String resourceId) {
        this.workflowStepName = workflowStepName;
        this.resourceId = resourceId;
    }

    /**
     * Create this resources created object with an StreamInput
     * @param input the input stream to read from
     * @throws IOException if failed to read input stream
     */
    public ResourceCreated(StreamInput input) throws IOException {
        this.workflowStepName = input.readString();
        this.resourceId = input.readString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject()
            .field(WORKFLOW_STEP_NAME, workflowStepName)
            .field(RESOURCE_ID_FIELD, resourceId);
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(workflowStepName);
        out.writeString(resourceId);
    }

    /**
     * Gets the resource id.
     *
     * @return the resourceId.
     */
    public String resourceId() {
        return resourceId;
    }

    /**
     * Gets the workflow step name associated to the created resource
     *
     * @return the workflowStepName.
     */
    public String workflowStepName() {
        return workflowStepName;
    }

    /**
     * Parse raw JSON content into a ResourceCreated instance.
     *
     * @param parser JSON based content parser
     * @return the parsed ResourceCreated instance
     * @throws IOException if content can't be parsed correctly
     */
    public static ResourceCreated parse(XContentParser parser) throws IOException {
        String workflowStepName = null;
        String resourceId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case WORKFLOW_STEP_NAME:
                    workflowStepName = parser.text();
                    break;
                case RESOURCE_ID_FIELD:
                    resourceId = parser.text();
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a resources_created object.");
            }
        }
        if (workflowStepName == null || resourceId == null) {
            throw new IOException("A ResourceCreated object requires both a workflowStepName and resourceId.");
        }
        return new ResourceCreated(workflowStepName, resourceId);
    }

    @Override
    public String toString() {
        return "resources_Created [resource_name=" + workflowStepName + ", id=" + resourceId + "]";
    }

}
