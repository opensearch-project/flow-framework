/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;

import java.io.IOException;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.RESOURCE_ID;
import static org.opensearch.flowframework.common.CommonValue.RESOURCE_TYPE;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STEP_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STEP_NAME;

/**
 * This represents an object in the WorkflowState {@link WorkflowState}.
 */
// TODO: create an enum to add the resource name itself for each step example (create_connector_step -> connector)
public class ResourceCreated implements ToXContentObject, Writeable {

    private static final Logger logger = LogManager.getLogger(ResourceCreated.class);

    private final String workflowStepName;
    private final String workflowStepId;
    private final String resourceType;
    private final String resourceId;

    /**
     * Create this resources created object with given workflow step name, ID and resource ID.
     * @param workflowStepName The workflow step name associating to the step where it was created
     * @param workflowStepId The workflow step ID associating to the step where it was created
     * @param resourceType The resource type
     * @param resourceId The resources ID for relating to the created resource
     */
    public ResourceCreated(String workflowStepName, String workflowStepId, String resourceType, String resourceId) {
        this.workflowStepName = workflowStepName;
        this.workflowStepId = workflowStepId;
        this.resourceType = resourceType;
        this.resourceId = resourceId;
    }

    /**
     * Create this resources created object with an StreamInput
     * @param input the input stream to read from
     * @throws IOException if failed to read input stream
     */
    public ResourceCreated(StreamInput input) throws IOException {
        this.workflowStepName = input.readString();
        this.workflowStepId = input.readString();
        this.resourceType = input.readString();
        this.resourceId = input.readString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject()
            .field(WORKFLOW_STEP_NAME, workflowStepName)
            .field(WORKFLOW_STEP_ID, workflowStepId)
            .field(RESOURCE_TYPE, resourceType)
            .field(RESOURCE_ID, resourceId);
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(workflowStepName);
        out.writeString(workflowStepId);
        out.writeString(resourceType);
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
     * Gets the resource type.
     *
     * @return the resource type.
     */
    public String resourceType() {
        return resourceType;
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
     * Gets the workflow step id associated to the created resource
     *
     * @return the workflowStepId.
     */
    public String workflowStepId() {
        return workflowStepId;
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
        String workflowStepId = null;
        String resourceType = null;
        String resourceId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case WORKFLOW_STEP_NAME:
                    workflowStepName = parser.text();
                    break;
                case WORKFLOW_STEP_ID:
                    workflowStepId = parser.text();
                    break;
                case RESOURCE_TYPE:
                    resourceType = parser.text();
                    break;
                case RESOURCE_ID:
                    resourceId = parser.text();
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a resources_created object.");
            }
        }
        if (workflowStepName == null) {
            logger.error("Resource created object failed parsing: workflowStepName: {}", workflowStepName);
            throw new FlowFrameworkException("A ResourceCreated object requires workflowStepName", RestStatus.BAD_REQUEST);
        }
        if (workflowStepId == null) {
            logger.error("Resource created object failed parsing: workflowStepId: {}", workflowStepId);
            throw new FlowFrameworkException("A ResourceCreated object requires workflowStepId", RestStatus.BAD_REQUEST);
        }
        if (resourceType == null) {
            logger.error("Resource created object failed parsing: resourceType: {}", resourceType);
            throw new FlowFrameworkException("A ResourceCreated object requires resourceType", RestStatus.BAD_REQUEST);
        }
        if (resourceId == null) {
            logger.error("Resource created object failed parsing: resourceId: {}", resourceId);
            throw new FlowFrameworkException("A ResourceCreated object requires resourceId", RestStatus.BAD_REQUEST);
        }
        return new ResourceCreated(workflowStepName, workflowStepId, resourceType, resourceId);
    }

    @Override
    public String toString() {
        return "resources_Created [workflow_step_name="
            + workflowStepName
            + ", workflow_step_id="
            + workflowStepId
            + ", resource_type="
            + resourceType
            + ", resource_id="
            + resourceId
            + "]";
    }
}
