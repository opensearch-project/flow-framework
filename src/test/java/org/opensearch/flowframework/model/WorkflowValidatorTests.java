/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.model;

import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.TASK_REQUEST_RETRY_DURATION;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowValidatorTests extends OpenSearchTestCase {

    private String validWorkflowStepJson;
    private String invalidWorkflowStepJson;
    private FlowFrameworkSettings flowFrameworkSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validWorkflowStepJson =
            "{\"workflow_step_1\":{\"inputs\":[\"input_1\",\"input_2\"],\"outputs\":[\"output_1\"]},\"workflow_step_2\":{\"inputs\":[\"input_1\",\"input_2\",\"input_3\"],\"outputs\":[\"output_1\",\"output_2\",\"output_3\"]}}";
        invalidWorkflowStepJson =
            "{\"workflow_step_1\":{\"bad_field\":[\"input_1\",\"input_2\"],\"outputs\":[\"output_1\"]},\"workflow_step_2\":{\"inputs\":[\"input_1\",\"input_2\",\"input_3\"],\"outputs\":[\"output_1\",\"output_2\",\"output_3\"]}}";

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);
    }

    public void testParseWorkflowValidator() throws IOException {
        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStep(),
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.DELETE_MODEL.getWorkflowStep(),
            WorkflowStepFactory.WorkflowSteps.DELETE_MODEL.getWorkflowStepValidator()
        );

        WorkflowValidator validator = new WorkflowValidator(workflowStepValidators);

        assertEquals(2, validator.getWorkflowStepValidators().size());
        assertTrue(validator.getWorkflowStepValidators().keySet().contains("create_connector"));
        assertEquals(7, validator.getWorkflowStepValidators().get("create_connector").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("create_connector").getOutputs().size());
        assertTrue(validator.getWorkflowStepValidators().keySet().contains("delete_model"));
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_model").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_model").getOutputs().size());
    }

    public void testWorkflowStepFactoryHasValidators() throws IOException {

        ThreadPool threadPool = mock(ThreadPool.class);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterAdminClient clusterAdminClient = mock(ClusterAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        MachineLearningNodeClient mlClient = mock(MachineLearningNodeClient.class);
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);

        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(FLOW_FRAMEWORK_ENABLED, MAX_WORKFLOWS, WORKFLOW_REQUEST_TIMEOUT, TASK_REQUEST_RETRY_DURATION)
        ).collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        WorkflowStepFactory workflowStepFactory = new WorkflowStepFactory(
            threadPool,
            clusterService,
            client,
            mlClient,
            flowFrameworkIndicesHandler,
            flowFrameworkSettings
        );

        WorkflowValidator workflowValidator = workflowStepFactory.getWorkflowValidator();

        // Get all workflow step validator types
        List<String> registeredWorkflowValidatorTypes = new ArrayList<String>(workflowValidator.getWorkflowStepValidators().keySet());

        // Get all registered workflow step types in the workflow step factory
        List<String> registeredWorkflowStepTypes = new ArrayList<String>(workflowStepFactory.getStepMap().keySet());

        // Check if each registered step has a corresponding validator definition
        assertTrue(registeredWorkflowStepTypes.containsAll(registeredWorkflowValidatorTypes));
        assertTrue(registeredWorkflowValidatorTypes.containsAll(registeredWorkflowStepTypes));
    }

}
