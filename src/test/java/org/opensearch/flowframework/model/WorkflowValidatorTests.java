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

    private FlowFrameworkSettings flowFrameworkSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        flowFrameworkSettings = mock(FlowFrameworkSettings.class);
        when(flowFrameworkSettings.isFlowFrameworkEnabled()).thenReturn(true);
    }

    public void testParseWorkflowValidator() throws IOException {
        Map<String, WorkflowStepValidator> workflowStepValidators = new HashMap<>();
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.CREATE_CONNECTOR.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.DELETE_MODEL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.DELETE_MODEL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.DEPLOY_MODEL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.DEPLOY_MODEL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.REGISTER_LOCAL_CUSTOM_MODEL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.REGISTER_LOCAL_CUSTOM_MODEL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.REGISTER_LOCAL_PRETRAINED_MODEL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.REGISTER_LOCAL_PRETRAINED_MODEL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.REGISTER_LOCAL_SPARSE_ENCODING_MODEL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.REGISTER_LOCAL_SPARSE_ENCODING_MODEL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.REGISTER_REMOTE_MODEL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.REGISTER_REMOTE_MODEL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.REGISTER_MODEL_GROUP.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.REGISTER_MODEL_GROUP.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.REGISTER_AGENT.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.REGISTER_AGENT.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.CREATE_TOOL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.CREATE_TOOL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.UNDEPLOY_MODEL.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.UNDEPLOY_MODEL.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.DELETE_CONNECTOR.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.DELETE_CONNECTOR.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.DELETE_AGENT.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.DELETE_AGENT.getWorkflowStepValidator()
        );
        workflowStepValidators.put(
            WorkflowStepFactory.WorkflowSteps.NOOP.getWorkflowStepName(),
            WorkflowStepFactory.WorkflowSteps.NOOP.getWorkflowStepValidator()
        );

        WorkflowValidator validator = new WorkflowValidator(workflowStepValidators);

        assertEquals(14, validator.getWorkflowStepValidators().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("create_connector"));
        assertEquals(7, validator.getWorkflowStepValidators().get("create_connector").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("create_connector").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("delete_model"));
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_model").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_model").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("deploy_model"));
        assertEquals(1, validator.getWorkflowStepValidators().get("deploy_model").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("deploy_model").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("register_remote_model"));
        assertEquals(2, validator.getWorkflowStepValidators().get("register_remote_model").getInputs().size());
        assertEquals(2, validator.getWorkflowStepValidators().get("register_remote_model").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("register_model_group"));
        assertEquals(1, validator.getWorkflowStepValidators().get("register_model_group").getInputs().size());
        assertEquals(2, validator.getWorkflowStepValidators().get("register_model_group").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("register_local_custom_model"));
        assertEquals(9, validator.getWorkflowStepValidators().get("register_local_custom_model").getInputs().size());
        assertEquals(2, validator.getWorkflowStepValidators().get("register_local_custom_model").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("register_local_sparse_encoding_model"));
        assertEquals(6, validator.getWorkflowStepValidators().get("register_local_sparse_encoding_model").getInputs().size());
        assertEquals(2, validator.getWorkflowStepValidators().get("register_local_sparse_encoding_model").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("register_local_pretrained_model"));
        assertEquals(3, validator.getWorkflowStepValidators().get("register_local_pretrained_model").getInputs().size());
        assertEquals(2, validator.getWorkflowStepValidators().get("register_local_pretrained_model").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("undeploy_model"));
        assertEquals(1, validator.getWorkflowStepValidators().get("undeploy_model").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("undeploy_model").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("delete_connector"));
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_connector").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_connector").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("register_agent"));
        assertEquals(2, validator.getWorkflowStepValidators().get("register_agent").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("register_agent").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("delete_agent"));
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_agent").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("delete_agent").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("create_tool"));
        assertEquals(1, validator.getWorkflowStepValidators().get("create_tool").getInputs().size());
        assertEquals(1, validator.getWorkflowStepValidators().get("create_tool").getOutputs().size());

        assertTrue(validator.getWorkflowStepValidators().keySet().contains("noop"));
        assertEquals(0, validator.getWorkflowStepValidators().get("noop").getInputs().size());
        assertEquals(0, validator.getWorkflowStepValidators().get("noop").getOutputs().size());

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
