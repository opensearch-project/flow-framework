/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.model.Template;

import java.io.IOException;

/**
 * Transport Response from getting a template
 */
public class GetTemplateResponse extends ActionResponse implements ToXContentObject {

    /** The template */
    private Template template;

    /**
     * Instantiates a new GetTemplateResponse from an input stream
     * @param in the input stream to read from
     * @throws IOException if the template json cannot be read from the input stream
     */
    public GetTemplateResponse(StreamInput in) throws IOException {
        super(in);
        this.template = Template.parse(in.readString());
    }

    /**
     * Instantiates a new GetTemplateResponse
     * @param template the template
     */
    public GetTemplateResponse(Template template) {
        this.template = template;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(template.toJson());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return this.template.toXContent(xContentBuilder, params);
    }

    /**
     * Gets the template
     * @return the template
     */
    public Template getTemplate() {
        return this.template;
    }

}
