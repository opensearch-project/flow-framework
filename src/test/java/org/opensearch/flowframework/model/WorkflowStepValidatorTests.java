/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.flowframework.workflow.CreateConnectorStep;
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

       var step = WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR;

        assertEquals(CreateConnectorStep.NAME,step.getWorkflowStepName());

        assertEquals(CreateConnectorStep.REQUIRED_INPUTS.size(),step.inputs().size());
        assertTrue(step.inputs().containsAll(CreateConnectorStep.REQUIRED_INPUTS));
        assertEquals(CreateConnectorStep.PROVIDED_OUTPUTS.size(),step.outputs().size());
        assertTrue(step.outputs().containsAll(CreateConnectorStep.PROVIDED_OUTPUTS));
    }

}
