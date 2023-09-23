/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.workflow.WorkflowData;
import org.opensearch.flowframework.workflow.WorkflowStep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This represents a process node (step) in a workflow graph in the {@link Template}.
 * It will have a one-to-one correspondence with a {@link ProcessNode},
 * where its type is used to determine the correct {@link WorkflowStep} object,
 * and its inputs are used to populate the {@link WorkflowData} input.
 */
public class WorkflowNode implements ToXContentObject {

    /** The template field name for node id */
    public static final String ID_FIELD = "id";
    /** The template field name for node type */
    public static final String TYPE_FIELD = "type";
    /** The template field name for node inputs */
    public static final String INPUTS_FIELD = "inputs";

    private final String id; // unique id
    private final String type; // maps to a WorkflowStep
    private final Map<String, Object> inputs; // maps to WorkflowData

    /**
     * Create this node with the id and type, and any user input.
     *
     * @param id A unique string identifying this node
     * @param type The type of {@link WorkflowStep} to create for the corresponding {@link ProcessNode}
     * @param inputs Optional input to populate params in {@link WorkflowData}
     */
    public WorkflowNode(String id, String type, Map<String, Object> inputs) {
        this.id = id;
        this.type = type;
        this.inputs = Map.copyOf(inputs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field(ID_FIELD, this.id);
        xContentBuilder.field(TYPE_FIELD, this.type);

        xContentBuilder.startObject(INPUTS_FIELD);
        for (Entry<String, Object> e : inputs.entrySet()) {
            xContentBuilder.field(e.getKey());
            if (e.getValue() instanceof String) {
                xContentBuilder.value(e.getValue());
            } else if (e.getValue() instanceof Map<?, ?>) {
                Template.buildStringToStringMap(xContentBuilder, (Map<?, ?>) e.getValue());
            } else if (e.getValue() instanceof Object[]) {
                // This assumes an array of maps for "processor" key
                xContentBuilder.startArray();
                for (Map<?, ?> map : (Map<?, ?>[]) e.getValue()) {
                    Template.buildStringToStringMap(xContentBuilder, map);
                }
                xContentBuilder.endArray();
            }
        }
        xContentBuilder.endObject();

        return xContentBuilder.endObject();
    }

    /**
     * Parse raw json content into a workflow node instance.
     *
     * @param parser json based content parser
     * @return the parsed WorkflowNode instance
     * @throws IOException if content can't be parsed correctly
     */
    public static WorkflowNode parse(XContentParser parser) throws IOException {
        String id = null;
        String type = null;
        Map<String, Object> inputs = new HashMap<>();

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
                        switch (parser.nextToken()) {
                            case VALUE_STRING:
                                inputs.put(inputFieldName, parser.text());
                                break;
                            case START_OBJECT:
                                inputs.put(inputFieldName, Template.parseStringToStringMap(parser));
                                break;
                            case START_ARRAY:
                                List<Map<String, String>> mapList = new ArrayList<>();
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    mapList.add(Template.parseStringToStringMap(parser));
                                }
                                inputs.put(inputFieldName, mapList.toArray(new Map[0]));
                                break;
                            default:
                                throw new IOException("Unable to parse field [" + inputFieldName + "] in a node object.");
                        }
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

    /**
     * Return this node's id
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * Return this node's type
     * @return the type
     */
    public String type() {
        return type;
    }

    /**
     * Return this node's input data
     * @return the inputs
     */
    public Map<String, Object> inputs() {
        return inputs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        WorkflowNode other = (WorkflowNode) obj;
        return Objects.equals(id, other.id);
    }

    @Override
    public String toString() {
        return this.id;
    }
}
