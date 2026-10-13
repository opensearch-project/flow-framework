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

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testParseWorkflowStepValidator() throws IOException {

        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStepValidator()
        );

        assertEquals(7, WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.inputs().size());
        assertEquals(1, WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.outputs().size());

        assertEquals("name", WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.inputs().get(0));
        assertEquals("connector_id", WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.outputs().get(0));
    }

}
