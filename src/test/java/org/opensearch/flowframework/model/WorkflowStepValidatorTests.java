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
import org.opensearch.flowframework.workflow.DeleteModelStep;
import org.opensearch.flowframework.workflow.DeployModelStep;
import org.opensearch.flowframework.workflow.RegisterModelGroupStep;
import org.opensearch.flowframework.workflow.RegisterRemoteModelStep;
import org.opensearch.flowframework.workflow.ReindexStep;
import org.opensearch.flowframework.workflow.UndeployModelStep;
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
        assertTrue("Enum inputs do not contain all expected inputs", stepEnum.inputs().containsAll(expectedInputs));

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

    public void testDeployModelStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.DEPLOY_MODEL,
            DeployModelStep.NAME,
            DeployModelStep.REQUIRED_INPUTS,
            DeployModelStep.PROVIDED_OUTPUTS
        );
    }

    public void testRegisterRemoteModelStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.REGISTER_REMOTE_MODEL,
            RegisterRemoteModelStep.NAME,
            RegisterRemoteModelStep.REQUIRED_INPUTS,
            RegisterRemoteModelStep.PROVIDED_OUTPUTS
        );
    }

    public void testReIndexStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.REINDEX,
            ReindexStep.NAME,
            ReindexStep.REQUIRED_INPUTS,
            ReindexStep.PROVIDED_OUTPUTS
        );
    }

    public void testRegisterModelGroupStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.REGISTER_MODEL_GROUP,
            RegisterModelGroupStep.NAME,
            RegisterModelGroupStep.REQUIRED_INPUTS,
            RegisterModelGroupStep.PROVIDED_OUTPUTS
        );
    }

    public void testUndeployModelStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.UNDEPLOY_MODEL,
            UndeployModelStep.NAME,
            UndeployModelStep.REQUIRED_INPUTS,
            UndeployModelStep.PROVIDED_OUTPUTS
        );
    }

    public void testDeleteModelStepValidator() throws IOException {
        assertStepValidatorMatches(
            WorkflowStepFactory.WorkflowSteps.DELETE_MODEL,
            DeleteModelStep.NAME,
            DeleteModelStep.REQUIRED_INPUTS,
            DeleteModelStep.PROVIDED_OUTPUTS
        );
    }

}
