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
import org.opensearch.flowframework.workflow.CreateIndexStep;
import org.opensearch.flowframework.workflow.DeleteIndexStep;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;

public class WorkflowStepValidatorTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    /* Universal helper method for synchronization check */
    private void assertStepValidatorMatches(
        WorkflowStepFactory.WorkflowSteps stepEnum,
        String expectedName,
        Set<String> expectedInputs,
        Set<String> expectedOutputs
    ) {
        // check name
        assertEquals("Step name mismatch", expectedName, stepEnum.getWorkflowStepName());

        // check inputs
        assertEquals("Input size mismatch", expectedInputs.size(), stepEnum.inputs().size());
        assertTrue("Enum inputs do not contain all expected outputs", stepEnum.inputs().containsAll(expectedInputs));

        // check outputs

        assertEquals("Output size mismatch", expectedOutputs.size(), stepEnum.outputs().size());
        assertTrue("Enum outputs do not contain all expected outputs", stepEnum.outputs().containsAll(expectedOutputs));
    }

    public void testCreateConnectorStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR,
            CreateConnectorStep.NAME,
            CreateConnectorStep.REQUIRED_INPUTS,
            CreateConnectorStep.PROVIDED_OUTPUTS
        );
    }

    public void testDeleteIndexStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.DELETE_INDEX,
            DeleteIndexStep.NAME,
            DeleteIndexStep.REQUIRED_INPUTS,
            DeleteIndexStep.PROVIDED_OUTPUTS
        );
    }

    public void testCreateIndexStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.CREATE_INDEX,
            CreateIndexStep.NAME,
            CreateIndexStep.REQUIRED_INPUTS,
            CreateIndexStep.PROVIDED_OUTPUTS
        );
    }

}
