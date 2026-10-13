/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * This represents an object of workflow steps json which maps each step to expected inputs and outputs
 */
public class WorkflowStepValidator implements ToXContentObject {

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
        for (String input : this.inputs) {
            xContentBuilder.value(input);
        }
        xContentBuilder.endArray();

        xContentBuilder.startArray(OUTPUTS_FIELD);
        for (String output : this.outputs) {
            xContentBuilder.value(output);
        }
        xContentBuilder.endArray();

        xContentBuilder.startArray(REQUIRED_PLUGINS);
        for (String rp : this.requiredPlugins) {
            xContentBuilder.value(rp);
        }
        xContentBuilder.endArray();

        if (timeout != null) {
            xContentBuilder.field(TIMEOUT, timeout);
        }

        return xContentBuilder.endObject();
    }
}
