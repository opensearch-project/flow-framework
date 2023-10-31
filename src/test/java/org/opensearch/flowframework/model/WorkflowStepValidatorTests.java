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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class WorkflowStepValidatorTests extends OpenSearchTestCase {

    private String validValidator;
    private String invalidValidator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validValidator = "{\"inputs\":[\"input_value\"],\"outputs\":[\"output_value\"]}";
        invalidValidator = "{\"inputs\":[\"input_value\"],\"invalid_field\":[\"output_value\"]}";
    }

    public void testParseWorkflowStepValidator() throws IOException {
        XContentParser parser = TemplateTestJsonUtil.jsonToParser(validValidator);
        WorkflowStepValidator workflowStepValidator = WorkflowStepValidator.parse(parser);

        assertEquals(1, workflowStepValidator.getInputs().size());
        assertEquals(1, workflowStepValidator.getOutputs().size());

        assertEquals("input_value", workflowStepValidator.getInputs().get(0));
        assertEquals("output_value", workflowStepValidator.getOutputs().get(0));
    }

    public void testFailedParseWorkflowStepValidator() throws IOException {
        XContentParser parser = TemplateTestJsonUtil.jsonToParser(invalidValidator);
        IOException ex = expectThrows(IOException.class, () -> WorkflowStepValidator.parse(parser));
        assertEquals("Unable to parse field [invalid_field] in a WorkflowStepValidator object.", ex.getMessage());

    }

}
