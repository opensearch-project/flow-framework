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
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

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

    private final String id; // unique id
    private final String type; // maps to a WorkflowStep
    private final Map<String, String> inputs; // maps to WorkflowData

    /**
     * Create this node with the id and type, and any user input.
     *
     * @param id A unique string identifying this node
     * @param type The type of {@link WorkflowStep} to create for the corresponding {@link ProcessNode}
     * @param inputs Optional input to populate params in {@link WorkflowData}
     */
    public WorkflowNode(String id, String type, Map<String, String> inputs) {
        this.id = id;
        this.type = type;
        this.inputs = inputs;
    }

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

    /**
     * Parse raw json content into a workflow node instance.
     *
     * @param parser json based content parser
     * @throws IOException if content can't be parsed correctly
     */
    public static WorkflowNode parse(XContentParser parser) throws IOException {
        String id = null;
        String type = null;
        Map<String, String> inputs = new HashMap<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case ID_FIELD:
                    id = parser.text();
                    break;
                case TYPE_FIELD:
                    type = parser.text();
                    break;
                case INPUTS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        String inputFieldName = parser.currentName();
                        parser.nextToken();
                        inputs.put(inputFieldName, parser.text());
                    }
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a node object.");
            }
        }
        if (id == null || type == null) {
            throw new IOException("An node object requires both an id and type field.");
        }

        return new WorkflowNode(id, type, inputs);
    }
}
