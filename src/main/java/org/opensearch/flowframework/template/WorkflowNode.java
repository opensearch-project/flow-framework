/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This represents a process node (step) in a workflow graph in the {@link Template}.
 * It will have a one-to-one correspondence with a {@link ProcessNode},
 * where its type is used to determine the correct {@link WorkflowStep} object,
 * and its inputs are used to populate the {@link WorkflowData} input.
 */
public class WorkflowNode implements ToXContentFragment {

    private static final String INPUTS_FIELD = "inputs";
    private static final String TYPE_FIELD = "type";
    private static final String ID_FIELD = "id";

    private String id = null; // unique id
    private String type = null; // maps to a WorkflowStep
    private Map<String, String> inputs = new HashMap<>(); // maps to WorkflowData

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(ID_FIELD, this.id);
        xContentBuilder.field(TYPE_FIELD, this.type);

        xContentBuilder.startObject(INPUTS_FIELD);
        for (Entry<String, String> e : inputs.entrySet()) {
            xContentBuilder.field(e.getKey(), e.getValue());
        }
        xContentBuilder.endObject();

        return xContentBuilder.endObject();
    }
}
