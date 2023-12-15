/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This represents an object of workflow steps json which maps each step to expected inputs and outputs
 */
public class WorkflowStepValidator {

    /** Inputs field name */
    private static final String INPUTS_FIELD = "inputs";
    /** Outputs field name */
    private static final String OUTPUTS_FIELD = "outputs";
    /** Required Plugins field name */
    private static final String REQUIRED_PLUGINS = "required_plugins";

    private List<String> inputs;
    private List<String> outputs;
    private List<String> requiredPlugins;

    /**
     * Intantiate the object representing a Workflow Step validator
     * @param inputs the workflow step inputs
     * @param outputs the workflow step outputs
     * @param requiredPlugins the required plugins for this workflow step
     */
    public WorkflowStepValidator(List<String> inputs, List<String> outputs, List<String> requiredPlugins) {
        this.inputs = inputs;
        this.outputs = outputs;
        this.requiredPlugins = requiredPlugins;
    }

    /**
     * Parse raw json content into a WorkflowStepValidator instance
     * @param parser json based content parser
     * @return an instance of the WorkflowStepValidator
     * @throws IOException if the content cannot be parsed correctly
     */
    public static WorkflowStepValidator parse(XContentParser parser) throws IOException {
        List<String> parsedInputs = new ArrayList<>();
        List<String> parsedOutputs = new ArrayList<>();
        List<String> requiredPlugins = new ArrayList<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case INPUTS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        parsedInputs.add(parser.text());
                    }
                    break;
                case OUTPUTS_FIELD:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        parsedOutputs.add(parser.text());
                    }
                    break;
                case REQUIRED_PLUGINS:
                    ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        requiredPlugins.add(parser.text());
                    }
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a WorkflowStepValidator object.");
            }
        }
        return new WorkflowStepValidator(parsedInputs, parsedOutputs, requiredPlugins);
    }

    /**
     * Get the required inputs
     * @return the inputs
     */
    public List<String> getInputs() {
        return List.copyOf(inputs);
    }

    /**
     * Get the required outputs
     * @return the outputs
     */
    public List<String> getOutputs() {
        return List.copyOf(outputs);
    }

    /**
     * Get the required plugins
     * @return the outputs
     */
    public List<String> getRequiredPlugins() {
        return List.copyOf(requiredPlugins);
    }
}
