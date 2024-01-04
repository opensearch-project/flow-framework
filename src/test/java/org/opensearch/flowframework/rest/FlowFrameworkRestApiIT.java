/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.opensearch.action.search.SearchResponse;
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
import org.opensearch.flowframework.model.WorkflowState;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkRestApiIT extends FlowFrameworkRestTestCase {

    public void testSearchWorkflows() throws Exception {

        // Create a Workflow that has a credential 12345
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // Retrieve WorkflowID
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        // Hit Search Workflows API
        String termIdQuery = "{\"query\":{\"ids\":{\"values\":[\"" + workflowId + "\"]}}}";
        SearchResponse searchResponse = searchWorkflows(termIdQuery);
        assertEquals(1, searchResponse.getHits().getTotalHits().value);

        String searchHitSource = searchResponse.getHits().getAt(0).getSourceAsString();
        Template searchHitTemplate = Template.parse(searchHitSource);

        // Confirm that credentials have been encrypted within the search response
        List<WorkflowNode> provisionNodes = searchHitTemplate.workflows().get(PROVISION_WORKFLOW).nodes();
        for (WorkflowNode node : provisionNodes) {
            if (node.type().equals("create_connector")) {
                @SuppressWarnings("unchecked")
                Map<String, String> credentialMap = new HashMap<>((Map<String, String>) node.userInputs().get(CREDENTIAL_FIELD));
                assertTrue(credentialMap.values().stream().allMatch(x -> x != "12345"));
            }
        }
    }

    public void testCreateAndProvisionLocalModelWorkflow() throws Exception {
        // Using a 2 step template to register a local model and deploy model
        Template template = TestHelpers.createTemplateFromFile("registerlocalmodel-deploymodel.json");
        // Remove deploy model input to test validation
        Workflow originalWorkflow = template.workflows().get(PROVISION_WORKFLOW);
        List<WorkflowNode> modifiednodes = originalWorkflow.nodes()
            .stream()
            .map(
                n -> "workflow_step_1".equals(n.id())
                    ? new WorkflowNode("workflow_step_1", "register_local_model", Collections.emptyMap(), Collections.emptyMap())
                    : n
            )
            .collect(Collectors.toList());
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
        assertTrue(exception.getMessage().contains("Invalid workflow, node [workflow_step_1] missing the following required inputs"));
        getAndAssertWorkflowStatus(workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);
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
        // This template should create 2 resources, registered_model_id and deployed model_id
        assertEquals(2, resourcesCreated.size());
        assertEquals("register_local_model", resourcesCreated.get(0).workflowStepName());
        assertNotNull(resourcesCreated.get(0).resourceId());
        assertEquals("deploy_model", resourcesCreated.get(1).workflowStepName());
        assertNotNull(resourcesCreated.get(1).resourceId());

        // Deprovision the workflow to avoid opening circut breaker when running additional tests
        Response deprovisionResponse = deprovisionWorkflow(workflowId);

        // wait for deprovision to complete
        Thread.sleep(5000);
    }

    public void testCreateAndProvisionRemoteModelWorkflow() throws Exception {

        // Using a 3 step template to create a connector, register remote model and deploy model
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");

        // Create cyclical graph to test dry run
        Workflow originalWorkflow = template.workflows().get(PROVISION_WORKFLOW);
        Workflow cyclicalWorkflow = new Workflow(
            originalWorkflow.userParams(),
            originalWorkflow.nodes(),
            List.of(new WorkflowEdge("workflow_step_2", "workflow_step_3"), new WorkflowEdge("workflow_step_3", "workflow_step_2"))
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
        ResponseException exception = expectThrows(ResponseException.class, () -> createWorkflowValidation(cyclicalTemplate));
        // output order not guaranteed
        assertTrue(exception.getMessage().contains("Cycle detected"));
        assertTrue(exception.getMessage().contains("workflow_step_2->workflow_step_3"));
        assertTrue(exception.getMessage().contains("workflow_step_3->workflow_step_2"));

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
        List<ResourceCreated> resourcesCreated = getResourcesCreated(workflowId, 30);

        // This template should create 3 resources, connector_id, registered model_id and deployed model_id
        assertEquals(3, resourcesCreated.size());
        assertEquals("create_connector", resourcesCreated.get(0).workflowStepName());
        assertNotNull(resourcesCreated.get(0).resourceId());
        assertEquals("register_remote_model", resourcesCreated.get(1).workflowStepName());
        assertNotNull(resourcesCreated.get(1).resourceId());
        assertEquals("deploy_model", resourcesCreated.get(2).workflowStepName());
        assertNotNull(resourcesCreated.get(2).resourceId());

        // Deprovision the workflow to avoid opening circut breaker when running additional tests
        Response deprovisionResponse = deprovisionWorkflow(workflowId);

        // wait for deprovision to complete
        Thread.sleep(5000);
    }

    public void testCreateAndProvisionDeployedRemoteModelWorkflow() throws Exception {

        // Using a 2 step template to create a connector, register remote model with deploy=true param set
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerdeployremotemodel.json");

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
        List<ResourceCreated> resourcesCreated = getResourcesCreated(workflowId, 30);

        // This template should create 3 resources, connector_id, registered model_id and deployed model_id
        assertEquals(3, resourcesCreated.size());
        assertEquals("create_connector", resourcesCreated.get(0).workflowStepName());
        assertNotNull(resourcesCreated.get(0).resourceId());
        assertEquals("register_remote_model", resourcesCreated.get(1).workflowStepName());
        assertNotNull(resourcesCreated.get(1).resourceId());
        assertEquals("deploy_model", resourcesCreated.get(2).workflowStepName());
        assertNotNull(resourcesCreated.get(2).resourceId());

        // Deprovision the workflow to avoid opening circut breaker when running additional tests
        Response deprovisionResponse = deprovisionWorkflow(workflowId);

        // wait for deprovision to complete
        Thread.sleep(5000);
    }

    public void testCreateAndProvisionAgentFrameworkWorkflow() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("agent-framework.json");

        // Hit Create Workflow API to create agent-framework template, with template validation check and provision parameter
        Response response = createWorkflowWithProvision(template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // wait and ensure state is completed/done
        assertBusy(() -> { getAndAssertWorkflowStatus(workflowId, State.COMPLETED, ProvisioningProgress.DONE); }, 30, TimeUnit.SECONDS);

        // Hit Search State API with the workflow id created above
        String query = "{\"query\":{\"ids\":{\"values\":[\"" + workflowId + "\"]}}}";
        SearchResponse searchResponse = searchWorkflowState(query);
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        String searchHitSource = searchResponse.getHits().getAt(0).getSourceAsString();
        WorkflowState searchHitWorkflowState = WorkflowState.parse(searchHitSource);

        // Assert based on the agent-framework template
        List<ResourceCreated> resourcesCreated = searchHitWorkflowState.resourcesCreated();
        Set<String> expectedStepNames = new HashSet<>();
        expectedStepNames.add("root_agent");
        expectedStepNames.add("sub_agent");
        expectedStepNames.add("openAI_connector");
        expectedStepNames.add("gpt-3.5-model");
        expectedStepNames.add("deployed-gpt-3.5-model");
        Set<String> stepNames = resourcesCreated.stream().map(ResourceCreated::workflowStepId).collect(Collectors.toSet());

        assertEquals(5, resourcesCreated.size());
        assertEquals(stepNames, expectedStepNames);
        assertNotNull(resourcesCreated.get(0).resourceId());

        // Hit Deprovision API
        Response deprovisionResponse = deprovisionWorkflow(workflowId);
        assertBusy(
            () -> { getAndAssertWorkflowStatus(workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
            60,
            TimeUnit.SECONDS
        );

        // Hit Delete API
        Response deleteResponse = deleteWorkflow(workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));

        // wait for deletion to complete
        Thread.sleep(30000);

        // Search this workflow id in global_context index to make sure it's deleted
        SearchResponse searchResponseAfterDeletion = searchWorkflows(query);
        assertBusy(() -> assertEquals(0, searchResponseAfterDeletion.getHits().getTotalHits().value), 30, TimeUnit.SECONDS);

    }

}
