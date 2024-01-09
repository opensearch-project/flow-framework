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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.exception.FlowFrameworkException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This represents an object of workflow steps json which maps each step to expected inputs and outputs
 */
public class WorkflowStepValidator implements ToXContentObject {

    private static final Logger logger = LogManager.getLogger(WorkflowStepValidator.class);

    /** Inputs field name */
    private static final String INPUTS_FIELD = "inputs";
    /** Outputs field name */
    private static final String OUTPUTS_FIELD = "outputs";
    /** Required Plugins field name */
    private static final String REQUIRED_PLUGINS = "required_plugins";
    /** Timeout field name */
    private static final String TIMEOUT = "timeout";

    private List<String> inputs;
    private List<String> outputs;
    private List<String> requiredPlugins;
    private TimeValue timeout;

    /**
     * Instantiate the object representing a Workflow Step validator
     * @param inputs the workflow step inputs
     * @param outputs the workflow step outputs
     * @param requiredPlugins the required plugins for this workflow step
     * @param timeout the timeout for this workflow step
     */
    public WorkflowStepValidator(List<String> inputs, List<String> outputs, List<String> requiredPlugins, TimeValue timeout) {
        this.inputs = inputs;
        this.outputs = outputs;
        this.requiredPlugins = requiredPlugins;
        this.timeout = timeout;
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
        TimeValue timeout = null;

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
                case TIMEOUT:
                    try {
                        timeout = TimeValue.parseTimeValue(parser.text(), TIMEOUT);
                    } catch (IllegalArgumentException e) {
                        logger.error("Failed to parse TIMEOUT value for field [{}]", fieldName, e);
                        throw new FlowFrameworkException(
                            "Failed to parse workflow-step.json file for field [" + fieldName + "]",
                            RestStatus.INTERNAL_SERVER_ERROR
                        );
                    }
                    break;
                default:
                    throw new IOException("Unable to parse field [" + fieldName + "] in a WorkflowStepValidator object.");
            }
        }
        return new WorkflowStepValidator(parsedInputs, parsedOutputs, requiredPlugins, timeout);
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
     * @return the required plugins
     */
    public List<String> getRequiredPlugins() {
        return List.copyOf(requiredPlugins);
    }

    /**
     * Get the timeout
     * @return the timeout
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.startArray(INPUTS_FIELD);
        for (String input: this.inputs) {
            xContentBuilder.value(input);
        }
        xContentBuilder.endArray();

        xContentBuilder.startArray(OUTPUTS_FIELD);
        for (String output: this.outputs) {
            xContentBuilder.value(output);
        }
        xContentBuilder.endArray();

        xContentBuilder.startArray(REQUIRED_PLUGINS);
        for (String rp: this.requiredPlugins) {
            xContentBuilder.value(rp);
        }
        xContentBuilder.endArray();

        xContentBuilder.field(TIMEOUT, timeout);

        return xContentBuilder.endObject();
    }
}
