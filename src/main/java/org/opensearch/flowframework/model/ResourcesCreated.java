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
import static org.opensearch.flowframework.common.CommonValue.RESOURCE_NAME_FIELD;

/**
 * This represents an object in the WorkflowState {@link WorkflowState}.
 */
public class ResourcesCreated implements ToXContentObject, Writeable {

    private String resourceName;
    private String resourceId;

    /**
     * Create this resources created object with given resource name and ID.
     * @param resourceName The resource name associating to the step name where it was created
     * @param resourceId The resources ID for relating to the created resource
     */
    public ResourcesCreated(String resourceName, String resourceId) {
        this.resourceName = resourceName;
        this.resourceId = resourceId;
    }

    /**
     * Create this resources created object with an StreamInput
     * @param input the input stream to read from
     * @throws IOException if failed to read input stream
     */
    public ResourcesCreated(StreamInput input) throws IOException {
        this.resourceName = input.readString();
        this.resourceId = input.readString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject()
            .field(RESOURCE_NAME_FIELD, resourceName)
            .field(RESOURCE_ID_FIELD, resourceId);
        return xContentBuilder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(resourceName);
        out.writeString(resourceId);
    }

    /**
     * Parse raw JSON content into a resourcesCreated instance.
     *
     * @param parser JSON based content parser
     * @return the parsed ResourcesCreated instance
     * @throws IOException if content can't be parsed correctly
     */
    public static ResourcesCreated parse(XContentParser parser) throws IOException {
        String resourceName = null;
        String resourceId = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case RESOURCE_NAME_FIELD:
                    resourceName = parser.text();
                    break;
                case RESOURCE_ID_FIELD:
                    resourceId = parser.text();
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a resources_created object.");
            }
        }
        return new ResourcesCreated(resourceName, resourceId);
    }

}
