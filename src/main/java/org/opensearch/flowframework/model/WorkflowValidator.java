/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * This represents the workflow steps json which maps each step to expected inputs and outputs
 */
public class WorkflowValidator implements ToXContentObject {

    private Map<String, WorkflowStepValidator> workflowStepValidators;

    /**
     * Intantiate the object representing a Workflow validator
     * @param workflowStepValidators a map of {@link WorkflowStepValidator}
     */
    public WorkflowValidator(Map<String, WorkflowStepValidator> workflowStepValidators) {
        this.workflowStepValidators = workflowStepValidators;
    }

    /**
     * Output this object in a compact JSON string.
     *
     * @return a JSON representation of the template.
     */
    public String toJson() {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            return this.toXContent(builder, EMPTY_PARAMS).toString();
        } catch (IOException e) {
            return "{\"error\": \"couldn't create JSON: " + e.getMessage() + "\"}";
        }
    }

    /**
     * Get the map of WorkflowStepValidators
     * @return the map of WorkflowStepValidators
     */
    public Map<String, WorkflowStepValidator> getWorkflowStepValidators() {
        return Map.copyOf(this.workflowStepValidators);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return xContentBuilder.map(workflowStepValidators);
    }
}
