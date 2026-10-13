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
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.RESOURCE_ID;
import static org.opensearch.flowframework.common.CommonValue.RESOURCE_TYPE;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STEP_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STEP_NAME;

/**
 * This represents an object in the WorkflowState {@link WorkflowState}.
 */
public class ResourceCreated implements ToXContentObject, Writeable {

    private static final Logger logger = LogManager.getLogger(ResourceCreated.class);

    private final Map<String, String> resourceMap;

    /**
     * Create this resources created object with given workflow step name, ID and resource ID.
     * @param workflowStepName The workflow step name associating to the step where it was created
     * @param workflowStepId The workflow step ID associating to the step where it was created
     * @param resourceType The resource type
     * @param resourceId The resources ID for relating to the created resource
     */
    public ResourceCreated(String workflowStepName, String workflowStepId, String resourceType, String resourceId) {
        this(
            Map.ofEntries(
                Map.entry(WORKFLOW_STEP_NAME, workflowStepName),
                Map.entry(WORKFLOW_STEP_ID, workflowStepId),
                Map.entry(RESOURCE_TYPE, resourceType),
                Map.entry(RESOURCE_ID, resourceId)
            )
        );
    }

    /**
     * Create this resources created object with an StreamInput
     * @param input the input stream to read from
     * @throws IOException if failed to read input stream
     */
    public ResourceCreated(StreamInput input) throws IOException {
        this(input.readMap(StreamInput::readString, StreamInput::readString));
    }

    private ResourceCreated(Map<String, String> map) {
        this.resourceMap = Map.copyOf(map);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(resourceMap);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(resourceMap, StreamOutput::writeString, StreamOutput::writeString);
    }

    /**
     * Gets the resource id.
     *
     * @return the resourceId.
     */
    public String resourceId() {
        return resourceMap.get(RESOURCE_ID);
    }

    /**
     * Gets the resource type.
     *
     * @return the resource type.
     */
    public String resourceType() {
        return resourceMap.get(RESOURCE_TYPE);
    }

    /**
     * Gets the workflow step name associated to the created resource
     *
     * @return the workflowStepName.
     */
    public String workflowStepName() {
        return resourceMap.get(WORKFLOW_STEP_NAME);
    }

    /**
     * Gets the workflow step id associated to the created resource
     *
     * @return the workflowStepId.
     */
    public String workflowStepId() {
        return resourceMap.get(WORKFLOW_STEP_ID);
    }

    /**
     * Gets the map of resource values
     *
     * @return a map with the resource values
     */
    public Map<String, String> resourceMap() {
        return resourceMap;
    }

    /**
     * Parse raw JSON content into a ResourceCreated instance.
     *
     * @param parser JSON based content parser
     * @return the parsed ResourceCreated instance
     * @throws IOException if content can't be parsed correctly
     */
    public static ResourceCreated parse(XContentParser parser) throws IOException {
        return new ResourceCreated(parser.mapStrings());
    }

    @Override
    public String toString() {
        return "resources_Created [" + resourceMap + "]";
    }
}
