/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.flowframework.workflow.WorkflowStepFactory.WorkflowSteps;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowValidatorTests extends OpenSearchTestCase {

    private FlowFrameworkSettings flowFrameworkSettings;
    private static Client client = mock(Client.class);

    @Override
    public void setUp() throws Exception {
        super.setUp();

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);
    }

    public void testParseWorkflowValidator() throws IOException {
        Map<String, WorkflowStepValidator> workflowStepValidators = Arrays.stream(WorkflowSteps.values())
            .collect(Collectors.toMap(WorkflowSteps::getWorkflowStepName, WorkflowSteps::getWorkflowStepValidator));

        WorkflowValidator validator = new WorkflowValidator(workflowStepValidators);

        assertEquals(24, validator.getWorkflowStepValidators().size());
    }

    public void testWorkflowStepFactoryHasValidators() throws IOException {

        ThreadPool threadPool = mock(ThreadPool.class);
        MachineLearningNodeClient mlClient = mock(MachineLearningNodeClient.class);
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        WorkflowStepFactory workflowStepFactory = new WorkflowStepFactory(
            threadPool,
            mlClient,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings,
            client
        );

        WorkflowValidator workflowValidator = workflowStepFactory.getWorkflowValidator();

        // Get all workflow step validator types
        List<String> registeredWorkflowValidatorTypes = new ArrayList<String>(workflowValidator.getWorkflowStepValidators().keySet());

        // Get all registered workflow step types in the workflow step factory
        List<String> registeredWorkflowStepTypes = new ArrayList<String>(workflowStepFactory.getStepMap().keySet());
        registeredWorkflowStepTypes.removeAll(WorkflowProcessSorter.WORKFLOW_STEP_DENYLIST);

        // Check if each registered step has a corresponding validator definition
        assertTrue(registeredWorkflowStepTypes.containsAll(registeredWorkflowValidatorTypes));
        assertTrue(registeredWorkflowValidatorTypes.containsAll(registeredWorkflowStepTypes));

        // Check JSON
        String json = workflowValidator.toJson();
        for (String step : registeredWorkflowStepTypes) {
            assertTrue(json.contains("\"" + step + "\""));
        }
    }
}
