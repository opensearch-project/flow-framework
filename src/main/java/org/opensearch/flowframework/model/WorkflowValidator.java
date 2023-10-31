/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.util.ParseUtils;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * This represents the workflow steps json which maps each step to expected inputs and outputs
 */
public class WorkflowValidator {

    private Map<String, WorkflowStepValidator> workflowStepValidators;

    /**
     * Intantiate the object representing a Workflow validator
     * @param workflowStepValidators a map of {@link WorkflowStepValidator}
     */
    public WorkflowValidator(Map<String, WorkflowStepValidator> workflowStepValidators) {
        this.workflowStepValidators = workflowStepValidators;
    }

    /**
     * Parse raw json content into a WorkflowValidator instance
     * @param parser json based content parser
     * @return an instance of the WorkflowValidator
     * @throws IOException if the content cannot be parsed correctly
     */
    public static WorkflowValidator parse(XContentParser parser) throws IOException {

        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String type = parser.currentName();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            workflowStepValidators.put(type, WorkflowStepValidator.parse(parser));
        }
        return new WorkflowValidator(workflowStepValidators);
    }

    /**
     * Parse a workflow step JSON file into a WorkflowValidator object
     *
     * @param file the file name of the workflow step json
     * @return A {@link WorkflowValidator} represented by the JSON
     * @throws IOException on failure to read and parse the json file
     */
    public static WorkflowValidator parse(String file) throws IOException {
        URL url = WorkflowValidator.class.getClassLoader().getResource(file);
        String json = Resources.toString(url, Charsets.UTF_8);
        return parse(ParseUtils.jsonToParser(json));
    }

    /**
     * Get the map of WorkflowStepValidators
     * @return the map of WorkflowStepValidators
     */
    public Map<String, WorkflowStepValidator> getWorkflowStepValidators() {
        return Map.copyOf(this.workflowStepValidators);
    }

}
