/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStep(),
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStepValidator()
        );

        assertEquals(7, WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getInputs().size());
        assertEquals(1, WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getOutputs().size());

        assertEquals("name", WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getInputs().get(0));
        assertEquals("connector_id", WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getOutputs().get(0));
    }

}
