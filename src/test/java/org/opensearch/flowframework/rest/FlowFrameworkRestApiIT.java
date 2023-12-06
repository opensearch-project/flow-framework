/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.FlowFrameworkRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkRestApiIT extends FlowFrameworkRestTestCase {

    public void testCreateAndProvisionLocalModelWorkflow() throws Exception {

        // Using a 3 step template to create a model group, register a remote model and deploy model
        Template template = TestHelpers.createTemplateFromFile("registermodelgroup-registerlocalmodel-deploymodel.json");

        // Remove register model input to test validation
        Workflow originalWorkflow = template.workflows().get(PROVISION_WORKFLOW);

        List<WorkflowNode> modifiednodes = new ArrayList<>();
        modifiednodes.add(
            new WorkflowNode(
                "workflow_step_1",
                "model_group",
                Map.of(),
                Map.of() // empty user inputs
            )
        );
        for (WorkflowNode node : originalWorkflow.nodes()) {
            if (!node.id().equals("workflow_step_1")) {
                modifiednodes.add(node);
            }
        }

        Workflow missingInputs = new Workflow(originalWorkflow.userParams(), modifiednodes, originalWorkflow.edges());

        Template templateWithMissingInputs = new Template.Builder().name(template.name())
            .description(template.description())
            .useCase(template.useCase())
            .templateVersion(template.templateVersion())
            .compatibilityVersion(template.compatibilityVersion())
            .workflows(Map.of(PROVISION_WORKFLOW, missingInputs))
            .uiMetadata(template.getUiMetadata())
            .user(template.getUser())
            .build();

        // Hit Create Workflow API with invalid template
        Response response = createWorkflow(templateWithMissingInputs);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // Retrieve workflow ID
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // Attempt provision
        ResponseException exception = expectThrows(ResponseException.class, () -> provisionWorkflow(workflowId));
        assertTrue(exception.getMessage().contains("Invalid graph, missing the following required inputs : [name]"));

        // update workflow with updated inputs
        response = updateWorkflow(workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // Reattempt Provision
        response = provisionWorkflow(workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(workflowId, 100);

        // TODO : This template should create 2 resources, model_group_id and model_id, need to fix after feature branch is merged
        assertEquals(0, resourcesCreated.size());
    }

    public void testCreateAndProvisionRemoteModelWorkflow() throws Exception {

        // Using a 3 step template to create a connector, register remote model and deploy model
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");

        // Create cyclical graph to test dry run
        Workflow originalWorkflow = template.workflows().get(PROVISION_WORKFLOW);
        Workflow cyclicalWorkflow = new Workflow(
            originalWorkflow.userParams(),
            originalWorkflow.nodes(),
            List.of(new WorkflowEdge("workflow_step_1", "workflow_step_2"), new WorkflowEdge("workflow_step_2", "workflow_step_1"))
        );

        Template cyclicalTemplate = new Template.Builder().name(template.name())
            .description(template.description())
            .useCase(template.useCase())
            .templateVersion(template.templateVersion())
            .compatibilityVersion(template.compatibilityVersion())
            .workflows(Map.of(PROVISION_WORKFLOW, cyclicalWorkflow))
            .uiMetadata(template.getUiMetadata())
            .user(template.getUser())
            .build();

        // Hit dry run
        ResponseException exception = expectThrows(ResponseException.class, () -> createWorkflowDryRun(cyclicalTemplate));
        assertTrue(exception.getMessage().contains("Cycle detected: [workflow_step_2->workflow_step_1, workflow_step_1->workflow_step_2]"));

        // Hit Create Workflow API with original template
        Response response = createWorkflow(template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // Hit Provision API and assert status
        response = provisionWorkflow(workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(workflowId, 10);

        // TODO : This template should create 2 resources, connector_id and model_id, need to fix after feature branch is merged
        assertEquals(1, resourcesCreated.size());
        assertEquals("create_connector", resourcesCreated.get(0).workflowStepName());
        assertNotNull(resourcesCreated.get(0).resourceId());
    }

}
