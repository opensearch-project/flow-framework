/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.Version;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.workflow.Workflow;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The Template is the central data structure which configures workflows. This object is used to parse JSON communicated via REST API.
 */
public class Template implements ToXContentObject {

    // TODO: Some of thse are placeholders based on the template design
    // Current code is only using user inputs and workflows
    private String name = null;
    private String description = null;
    private String useCase = null;
    private String[] operations = null; // probably an ENUM actually
    private Version templateVersion = null;
    private Version[] compatibilityVersion = null;
    private Map<String, String> userInputs = new HashMap<>();
    private Map<String, Workflow> workflows = new HashMap<>();

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field("name", this.name);
        xContentBuilder.field("description", this.description);
        xContentBuilder.field("use_case", this.useCase);
        xContentBuilder.field("operations", this.operations);

        xContentBuilder.startObject("version");
        xContentBuilder.field("template", this.templateVersion);
        xContentBuilder.startArray("compatibility");
        for (Version v : this.compatibilityVersion) {
            xContentBuilder.value(v);
        }
        xContentBuilder.endArray();
        xContentBuilder.endObject();

        xContentBuilder.startObject("user_inputs");
        for (Entry<String, String> e : userInputs.entrySet()) {
            xContentBuilder.field(e.getKey(), e.getValue());
        }
        xContentBuilder.endObject();

        xContentBuilder.startObject("workflows");
        for (Entry<String, Workflow> e : workflows.entrySet()) {
            xContentBuilder.field(e.getKey(), e.getValue(), params);
        }
        xContentBuilder.endObject();

        return xContentBuilder.endObject();
    }

}
