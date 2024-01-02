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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.FlowFrameworkSettings.FLOW_FRAMEWORK_ENABLED;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_GET_TASK_REQUEST_RETRY;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_WORKFLOWS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.WORKFLOW_REQUEST_TIMEOUT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkflowValidatorTests extends OpenSearchTestCase {

    private String validWorkflowStepJson;
    private String invalidWorkflowStepJson;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validWorkflowStepJson =
            "{\"workflow_step_1\":{\"inputs\":[\"input_1\",\"input_2\"],\"outputs\":[\"output_1\"]},\"workflow_step_2\":{\"inputs\":[\"input_1\",\"input_2\",\"input_3\"],\"outputs\":[\"output_1\",\"output_2\",\"output_3\"]}}";
        invalidWorkflowStepJson =
            "{\"workflow_step_1\":{\"bad_field\":[\"input_1\",\"input_2\"],\"outputs\":[\"output_1\"]},\"workflow_step_2\":{\"inputs\":[\"input_1\",\"input_2\",\"input_3\"],\"outputs\":[\"output_1\",\"output_2\",\"output_3\"]}}";
    }

    public void testParseWorkflowValidator() throws IOException {

        XContentParser parser = TemplateTestJsonUtil.jsonToParser(validWorkflowStepJson);
        WorkflowValidator validator = WorkflowValidator.parse(parser);

        assertEquals(2, validator.getWorkflowStepValidators().size());
        assertTrue(validator.getWorkflowStepValidators().keySet().contains("workflow_step_1"));
        assertEquals(2, validator.getWorkflowStepValidators().get("workflow_step_1").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("workflow_step_1").getOutputs().size());
        assertTrue(validator.getWorkflowStepValidators().keySet().contains("workflow_step_2"));
        assertEquals(3, validator.getWorkflowStepValidators().get("workflow_step_2").getInputs().size());
        assertEquals(3, validator.getWorkflowStepValidators().get("workflow_step_2").getOutputs().size());
    }

    public void testFailedParseWorkflowValidator() throws IOException {
        XContentParser parser = TemplateTestJsonUtil.jsonToParser(invalidWorkflowStepJson);
        IOException ex = expectThrows(IOException.class, () -> WorkflowValidator.parse(parser));
        assertEquals("Unable to parse field [bad_field] in a WorkflowStepValidator object.", ex.getMessage());
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
            Stream.of(FLOW_FRAMEWORK_ENABLED, MAX_WORKFLOWS, WORKFLOW_REQUEST_TIMEOUT, MAX_GET_TASK_REQUEST_RETRY)
        ).collect(Collectors.toSet());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        WorkflowStepFactory workflowStepFactory = new WorkflowStepFactory(
            Settings.EMPTY,
            threadPool,
            clusterService,
            client,
            mlClient,
            flowFrameworkIndicesHandler
        );

        // Read in workflow-steps.json
        WorkflowValidator workflowValidator = WorkflowValidator.parse("mappings/workflow-steps.json");

        // Get all workflow step validator types
        List<String> registeredWorkflowValidatorTypes = new ArrayList<String>(workflowValidator.getWorkflowStepValidators().keySet());

        // Get all registered workflow step types in the workflow step factory
        List<String> registeredWorkflowStepTypes = new ArrayList<String>(workflowStepFactory.getStepMap().keySet());

        // Check if each registered step has a corresponding validator definition
        assertTrue(registeredWorkflowStepTypes.containsAll(registeredWorkflowValidatorTypes));
        assertTrue(registeredWorkflowValidatorTypes.containsAll(registeredWorkflowStepTypes));
    }

}
