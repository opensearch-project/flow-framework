/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.apache.http.util.EntityUtils;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.FlowFrameworkRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.DefaultUseCases;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowEdge;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.flowframework.model.WorkflowState;
import org.junit.Before;
import org.junit.ComparisonFailure;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.CREATE_CONNECTOR_CREDENTIAL_KEY;
import static org.opensearch.flowframework.common.CommonValue.CREATE_INGEST_PIPELINE_MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkRestApiIT extends FlowFrameworkRestTestCase {

    @Before
    public void waitToStart() throws Exception {
        // ML Commons cron job runs every 10 seconds and takes 20+ seconds to initialize .plugins-ml-config index
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
        }
    }

    public void testSearchWorkflows() throws Exception {
        // Create a Workflow that has a credential 12345
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // Retrieve WorkflowID
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        // Hit Search Workflows API
        String termIdQuery = "{\"query\":{\"ids\":{\"values\":[\"" + workflowId + "\"]}}}";
        SearchResponse searchResponse = searchWorkflows(client(), termIdQuery);
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
    }

    public void testFailedUpdateWorkflow() throws Exception {
        Template templateCreation = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response responseCreate = createWorkflow(client(), templateCreation);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(responseCreate));

        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");

        ResponseException exception = expectThrows(ResponseException.class, () -> updateWorkflow(client(), "123", template));
        assertTrue(exception.getMessage().contains("Failed to retrieve template (123) from global context."));

        Response response = createWorkflow(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        Response provisionResponse = provisionResponse = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(provisionResponse));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Failed update since provisioning has started
        ResponseException exceptionProvisioned = expectThrows(
            ResponseException.class,
            () -> updateWorkflow(client(), workflowId, template)
        );
        assertTrue(
            exceptionProvisioned.getMessage().contains("The template can not be updated unless its provisioning state is NOT_STARTED")
        );
    }

    public void testUpdateWorkflowUsingFields() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        Response provisionResponse = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(provisionResponse));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Attempt to update with update_fields with illegal field
        // Fails because contains workflow field
        ResponseException exceptionProvisioned = expectThrows(
            ResponseException.class,
            () -> updateWorkflowWithFields(client(), workflowId, "{\"workflows\":{}}")
        );
        assertTrue(
            exceptionProvisioned.getMessage().contains("You can not update the field [workflows] without updating the whole template.")
        );
        // Change just the name and description
        response = updateWorkflowWithFields(client(), workflowId, "{\"name\":\"foo\",\"description\":\"bar\"}");
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        // Get the updated template
        response = getWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        Template t = Template.parse(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        assertEquals("foo", t.name());
        assertEquals("bar", t.description());
    }

    public void testCreateAndProvisionLocalModelWorkflow() throws Exception {
        // Using a 1 step template to register a local model and deploy model
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");

        // Remove register model input to test validation
        Workflow originalWorkflow = template.workflows().get(PROVISION_WORKFLOW);
        List<WorkflowNode> modifiednodes = originalWorkflow.nodes()
            .stream()
            .map(
                n -> "workflow_step_1".equals(n.id())
                    ? new WorkflowNode(
                        "workflow_step_1",
                        "register_local_sparse_encoding_model",
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    )
                    : n
            )
            .collect(Collectors.toList());
        Workflow missingInputs = new Workflow(originalWorkflow.userParams(), modifiednodes, originalWorkflow.edges());
        Template templateWithMissingInputs = Template.builder(template).workflows(Map.of(PROVISION_WORKFLOW, missingInputs)).build();

        // Hit Create Workflow API with invalid template
        Response response = createWorkflow(client(), templateWithMissingInputs);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        getAndAssertWorkflowStep(client());

        // Retrieve workflow ID
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // Attempt provision
        ResponseException exception = expectThrows(ResponseException.class, () -> provisionWorkflow(client(), workflowId));
        assertTrue(exception.getMessage().contains("Invalid workflow, node [workflow_step_1] missing the following required inputs"));
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // update workflow with updated inputs
        response = updateWorkflow(client(), workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // Reattempt Provision
        response = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 100);

        // This template should create 2 resources, registered_model_id and deployed model_id
        assertEquals(2, resourcesCreated.size());
        assertEquals("register_local_sparse_encoding_model", resourcesCreated.get(0).workflowStepName());
        assertNotNull(resourcesCreated.get(0).resourceId());
        assertEquals("deploy_model", resourcesCreated.get(1).workflowStepName());
        assertNotNull(resourcesCreated.get(1).resourceId());

        // Delete the workflow without deleting the resources
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));

        // Verify state doc is not deleted
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            30,
            TimeUnit.SECONDS
        );
    }

    public void testCreateAndProvisionCyclicalTemplate() throws Exception {

        // Using a 3 step template to create a connector, register remote model and deploy model
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");

        // Create cyclical graph to test dry run
        Workflow originalWorkflow = template.workflows().get(PROVISION_WORKFLOW);
        Workflow cyclicalWorkflow = new Workflow(
            originalWorkflow.userParams(),
            originalWorkflow.nodes(),
            List.of(new WorkflowEdge("workflow_step_2", "workflow_step_3"), new WorkflowEdge("workflow_step_3", "workflow_step_2"))
        );

        Template cyclicalTemplate = Template.builder(template).workflows(Map.of(PROVISION_WORKFLOW, cyclicalWorkflow)).build();

        // Hit dry run
        ResponseException exception = expectThrows(ResponseException.class, () -> createWorkflowValidation(client(), cyclicalTemplate));
        // output order not guaranteed
        assertTrue(exception.getMessage().contains("Cycle detected"));
        assertTrue(exception.getMessage().contains("workflow_step_2->workflow_step_3"));
        assertTrue(exception.getMessage().contains("workflow_step_3->workflow_step_2"));
    }

    public void testCreateAndProvisionRemoteModelWorkflow() throws Exception {
        // Using a 3 step template to create a connector, register remote model and deploy model
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");

        // Hit Create Workflow API with original template
        Response response = createWorkflow(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        response = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 30);

        // This template should create 3 resources, connector_id, registered model_id and deployed model_id
        assertEquals(3, resourcesCreated.size());
        assertEquals("create_connector", resourcesCreated.get(0).workflowStepName());
        assertNotNull(resourcesCreated.get(0).resourceId());
        assertEquals("register_remote_model", resourcesCreated.get(1).workflowStepName());
        assertNotNull(resourcesCreated.get(1).resourceId());
        assertEquals("deploy_model", resourcesCreated.get(2).workflowStepName());
        assertNotNull(resourcesCreated.get(2).resourceId());

        // Delete the workflow without deleting the resources
        Response deleteResponse = deleteWorkflow(client(), workflowId, "?clear_status=true");
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));

        // Verify state doc is deleted
        assertBusy(() -> { getAndAssertWorkflowStatusNotFound(client(), workflowId); }, 30, TimeUnit.SECONDS);
    }

    public void testCreateAndProvisionAgentFrameworkWorkflow() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("agent-framework.json");

        // Hit Create Workflow API to create agent-framework template, with template validation check and provision parameter
        Response response = createWorkflowWithProvision(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            120,
            TimeUnit.SECONDS
        );

        // Hit Search State API with the workflow id created above
        String query = "{\"query\":{\"ids\":{\"values\":[\"" + workflowId + "\"]}}}";
        SearchResponse searchResponse = searchWorkflowState(client(), query);
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
        Set<String> stepNames = resourcesCreated.stream().map(ResourceCreated::workflowStepId).collect(Collectors.toSet());

        assertEquals(5, resourcesCreated.size());
        assertEquals(stepNames, expectedStepNames);
        assertNotNull(resourcesCreated.get(0).resourceId());

        // Hit Deprovision API
        // By design, this may not completely deprovision the first time if it takes >2s to process removals
        Response deprovisionResponse = deprovisionWorkflow(client(), workflowId);
        try {
            assertBusy(
                () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
                30,
                TimeUnit.SECONDS
            );
        } catch (ComparisonFailure e) {
            // 202 return if still processing
            assertEquals(RestStatus.ACCEPTED, TestHelpers.restStatus(deprovisionResponse));
        }
        if (TestHelpers.restStatus(deprovisionResponse) == RestStatus.ACCEPTED) {
            // Short wait before we try again
            Thread.sleep(10000);
            deprovisionResponse = deprovisionWorkflow(client(), workflowId);
            assertBusy(
                () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
                30,
                TimeUnit.SECONDS
            );
        }
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprovisionResponse));
        // Hit Delete API
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));

        // Verify state doc is deleted
        assertBusy(() -> { getAndAssertWorkflowStatusNotFound(client(), workflowId); }, 30, TimeUnit.SECONDS);
    }

    public void testCreateAndProvisionConnectorToolAgentFrameworkWorkflow() throws Exception {
        // Create a Workflow that has a credential 12345
        Template template = TestHelpers.createTemplateFromFile("createconnector-createconnectortool-createflowagent.json");

        // Hit Create Workflow API to create agent-framework template, with template validation check and provision parameter
        Response response = createWorkflowWithProvision(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            120,
            TimeUnit.SECONDS
        );

        // Assert based on the agent-framework template
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 120);
        Map<String, ResourceCreated> resourceMap = resourcesCreated.stream()
            .collect(Collectors.toMap(ResourceCreated::workflowStepName, r -> r));
        assertEquals(2, resourceMap.size());
        assertTrue(resourceMap.containsKey("create_connector"));
        assertTrue(resourceMap.containsKey("register_agent"));
        String connectorId = resourceMap.get("create_connector").resourceId();
        String agentId = resourceMap.get("register_agent").resourceId();
        assertNotNull(connectorId);
        assertNotNull(agentId);

        // Assert that the agent contains the correct connector_id
        response = getAgent(client(), agentId);
        Map<String, Object> agentResponse = entityAsMap(response);
        assertTrue(agentResponse.containsKey("tools"));
        @SuppressWarnings("unchecked")
        ArrayList<Map<String, Object>> tools = (ArrayList<Map<String, Object>>) agentResponse.get("tools");
        assertEquals(1, tools.size());
        Map<String, Object> tool = tools.get(0);
        assertTrue(tool.containsKey("parameters"));
        @SuppressWarnings("unchecked")
        Map<String, String> toolParameters = (Map<String, String>) tool.get("parameters");
        assertEquals(toolParameters, Map.of("connector_id", connectorId));

        // Hit Deprovision API
        // By design, this may not completely deprovision the first time if it takes >2s to process removals
        Response deprovisionResponse = deprovisionWorkflow(client(), workflowId);
        try {
            assertBusy(
                () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
                30,
                TimeUnit.SECONDS
            );
        } catch (ComparisonFailure e) {
            // 202 return if still processing
            assertEquals(RestStatus.ACCEPTED, TestHelpers.restStatus(deprovisionResponse));
        }
        if (TestHelpers.restStatus(deprovisionResponse) == RestStatus.ACCEPTED) {
            // Short wait before we try again
            Thread.sleep(10000);
            deprovisionResponse = deprovisionWorkflow(client(), workflowId);
            assertBusy(
                () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
                30,
                TimeUnit.SECONDS
            );
        }
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprovisionResponse));
        // Hit Delete API
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));

        // Verify state doc is deleted
        assertBusy(() -> { getAndAssertWorkflowStatusNotFound(client(), workflowId); }, 30, TimeUnit.SECONDS);
    }

    public void testReprovisionWorkflow() throws Exception {
        // Begin with a template to register a local pretrained model
        Template template = TestHelpers.createTemplateFromFile("registerremotemodel.json");

        Response response = createWorkflowWithProvision(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            120,
            TimeUnit.SECONDS
        );

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 30);
        assertEquals(3, resourcesCreated.size());
        Map<String, ResourceCreated> resourceMap = resourcesCreated.stream()
            .collect(Collectors.toMap(ResourceCreated::workflowStepName, r -> r));
        assertTrue(resourceMap.containsKey("create_connector"));
        assertTrue(resourceMap.containsKey("register_remote_model"));

        // Reprovision template to add ingest pipeline which uses the model ID
        template = TestHelpers.createTemplateFromFile("registerremotemodel-ingestpipeline.json");
        response = reprovisionWorkflow(client(), workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        resourcesCreated = getResourcesCreated(client(), workflowId, 30);
        assertEquals(4, resourcesCreated.size());
        resourceMap = resourcesCreated.stream().collect(Collectors.toMap(ResourceCreated::workflowStepName, r -> r));
        assertTrue(resourceMap.containsKey("create_connector"));
        assertTrue(resourceMap.containsKey("register_remote_model"));
        assertTrue(resourceMap.containsKey("create_ingest_pipeline"));

        // Retrieve pipeline by ID to ensure model ID is set correctly
        String modelId = resourceMap.get("register_remote_model").resourceId();
        String pipelineId = resourceMap.get("create_ingest_pipeline").resourceId();
        GetPipelineResponse getPipelineResponse = getPipelines(pipelineId);
        assertEquals(1, getPipelineResponse.pipelines().size());
        assertTrue(getPipelineResponse.pipelines().get(0).getConfigAsMap().toString().contains(modelId));

        // Reprovision template to add index which uses default ingest pipeline
        Instant preUpdateTime = Instant.now(); // Store a timestamp
        template = TestHelpers.createTemplateFromFile("registerremotemodel-ingestpipeline-createindex.json");
        response = reprovisionWorkflow(client(), workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        resourcesCreated = getResourcesCreated(client(), workflowId, 30);
        assertEquals(5, resourcesCreated.size());
        resourceMap = resourcesCreated.stream().collect(Collectors.toMap(ResourceCreated::workflowStepName, r -> r));
        assertTrue(resourceMap.containsKey("create_connector"));
        assertTrue(resourceMap.containsKey("register_remote_model"));
        assertTrue(resourceMap.containsKey("create_ingest_pipeline"));
        assertTrue(resourceMap.containsKey("create_index"));

        // Retrieve index settings to ensure pipeline ID is set correctly
        String indexName = resourceMap.get("create_index").resourceId();
        Map<String, Object> indexSettings = getIndexSettingsAsMap(indexName);
        assertEquals(pipelineId, indexSettings.get("index.default_pipeline"));

        // The template doesn't get updated until after the resources are created which can cause a race condition and flaky failure
        // See https://github.com/opensearch-project/flow-framework/issues/870
        // Making sure the template got updated before reprovisioning again.
        // Quick fix to stop this from being flaky, needs a more permanent fix to synchronize template update with COMPLETED provisioning
        assertBusy(() -> {
            Response r = getWorkflow(client(), workflowId);
            assertEquals(RestStatus.OK.getStatus(), r.getStatusLine().getStatusCode());
            Template t = Template.parse(EntityUtils.toString(r.getEntity(), StandardCharsets.UTF_8));
            assertTrue(t.lastUpdatedTime().isAfter(preUpdateTime));
        }, 30, TimeUnit.SECONDS);

        // Reprovision template to remove default ingest pipeline
        template = TestHelpers.createTemplateFromFile("registerremotemodel-ingestpipeline-updateindex.json");
        response = reprovisionWorkflow(client(), workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        resourcesCreated = getResourcesCreated(client(), workflowId, 30);
        // resource count should remain unchanged when updating an existing node
        assertEquals(5, resourcesCreated.size());

        // Retrieve index settings to ensure default pipeline has been updated correctly
        indexSettings = getIndexSettingsAsMap(indexName);
        assertEquals("_none", indexSettings.get("index.default_pipeline"));

        // Deprovision and delete all resources
        Response deprovisionResponse = deprovisionWorkflowWithAllowDelete(client(), workflowId, pipelineId + "," + indexName);
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
            60,
            TimeUnit.SECONDS
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprovisionResponse));

        // Hit Delete API
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));
    }

    public void testReprovisionWorkflowMidNodeAddition() throws Exception {
        // Begin with a template to register a local pretrained model and create an index, no edges
        Template template = TestHelpers.createTemplateFromFile("registerremotemodel-createindex.json");

        Response response = createWorkflowWithProvision(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            120,
            TimeUnit.SECONDS
        );

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 30);
        assertEquals(4, resourcesCreated.size());
        Map<String, ResourceCreated> resourceMap = resourcesCreated.stream()
            .collect(Collectors.toMap(ResourceCreated::workflowStepName, r -> r));
        assertTrue(resourceMap.containsKey("create_connector"));
        assertTrue(resourceMap.containsKey("register_remote_model"));
        assertTrue(resourceMap.containsKey("create_index"));

        // Reprovision template to add ingest pipeline which uses the model ID
        template = TestHelpers.createTemplateFromFile("registerremotemodel-ingestpipeline-createindex.json");
        response = reprovisionWorkflow(client(), workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        resourcesCreated = getResourcesCreated(client(), workflowId, 30);
        assertEquals(5, resourcesCreated.size());
        resourceMap = resourcesCreated.stream().collect(Collectors.toMap(ResourceCreated::workflowStepName, r -> r));
        assertTrue(resourceMap.containsKey("create_connector"));
        assertTrue(resourceMap.containsKey("register_remote_model"));
        assertTrue(resourceMap.containsKey("create_ingest_pipeline"));
        assertTrue(resourceMap.containsKey("create_index"));

        // Ensure ingest pipeline configuration contains the model id and index settings have the ingest pipeline as default
        String modelId = resourceMap.get("register_remote_model").resourceId();
        String pipelineId = resourceMap.get("create_ingest_pipeline").resourceId();
        GetPipelineResponse getPipelineResponse = getPipelines(pipelineId);
        assertEquals(1, getPipelineResponse.pipelines().size());
        assertTrue(getPipelineResponse.pipelines().get(0).getConfigAsMap().toString().contains(modelId));

        String indexName = resourceMap.get("create_index").resourceId();
        Map<String, Object> indexSettings = getIndexSettingsAsMap(indexName);
        assertEquals(pipelineId, indexSettings.get("index.default_pipeline"));

        // Deprovision and delete all resources
        Response deprovisionResponse = deprovisionWorkflowWithAllowDelete(client(), workflowId, pipelineId + "," + indexName);
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
            60,
            TimeUnit.SECONDS
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprovisionResponse));

        // Hit Delete API
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));
    }

    public void testReprovisionWithNoChange() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("registerremotemodel-ingestpipeline-createindex.json");

        Response response = createWorkflowWithProvision(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            120,
            TimeUnit.SECONDS
        );

        // Attempt to reprovision the same template with no changes
        ResponseException exception = expectThrows(ResponseException.class, () -> reprovisionWorkflow(client(), workflowId, template));
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        assertTrue(exception.getMessage().contains("Template does not contain any modifications"));

        // Deprovision and delete all resources
        Response deprovisionResponse = deprovisionWorkflowWithAllowDelete(
            client(),
            workflowId,
            "nlp-ingest-pipeline" + "," + "my-nlp-index"
        );
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
            60,
            TimeUnit.SECONDS
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprovisionResponse));

        // Hit Delete API
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));
    }

    public void testReprovisionWithDeletion() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("registerremotemodel-ingestpipeline-createindex.json");

        Response response = createWorkflowWithProvision(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            120,
            TimeUnit.SECONDS
        );

        // Attempt to reprovision template without ingest pipeline node
        Template templateWithoutIngestPipeline = TestHelpers.createTemplateFromFile("registerremotemodel-createindex.json");
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> reprovisionWorkflow(client(), workflowId, templateWithoutIngestPipeline)
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        assertTrue(exception.getMessage().contains("Workflow Step deletion is not supported when reprovisioning a template."));

        // Deprovision and delete all resources
        Response deprovisionResponse = deprovisionWorkflowWithAllowDelete(
            client(),
            workflowId,
            "nlp-ingest-pipeline" + "," + "my-nlp-index"
        );
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
            60,
            TimeUnit.SECONDS
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprovisionResponse));

        // Hit Delete API
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));
    }

    public void testTimestamps() throws Exception {
        Template noopTemplate = TestHelpers.createTemplateFromFile("noop.json");
        // Create the template, should have created and updated matching
        Response response = createWorkflow(client(), noopTemplate);
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        assertNotNull(workflowId);

        response = getWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        Template t = Template.parse(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        Instant createdTime = t.createdTime();
        Instant lastUpdatedTime = t.lastUpdatedTime();
        assertNotNull(createdTime);
        assertEquals(createdTime, lastUpdatedTime);
        assertNull(t.lastProvisionedTime());

        // Update the template, should have created same as before and updated newer
        response = updateWorkflow(client(), workflowId, noopTemplate);
        assertEquals(RestStatus.CREATED.getStatus(), response.getStatusLine().getStatusCode());

        response = getWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        t = Template.parse(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        assertEquals(createdTime, t.createdTime());
        assertTrue(t.lastUpdatedTime().isAfter(lastUpdatedTime));
        lastUpdatedTime = t.lastUpdatedTime();
        assertNull(t.lastProvisionedTime());

        // Provision the template, should have created and updated same as before and provisioned newer
        response = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

        response = getWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        t = Template.parse(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        assertEquals(createdTime, t.createdTime());
        assertEquals(lastUpdatedTime, t.lastUpdatedTime());
        assertTrue(t.lastProvisionedTime().isAfter(lastUpdatedTime));

        // Clean up
        response = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
    }

    public void testCreateAndProvisionIngestAndSearchPipeline() throws Exception {

        // Using a 3 step template to create a connector, register remote model and deploy model
        Template template = TestHelpers.createTemplateFromFile("ingest-search-pipeline-template.json");

        // Hit Create Workflow API with original template
        Response response = createWorkflow(client(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        response = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 30);

        List<String> expectedStepNames = List.of(
            "create_connector",
            "register_remote_model",
            "deploy_model",
            "create_search_pipeline",
            "create_ingest_pipeline"
        );

        List<String> workflowStepNames = resourcesCreated.stream()
            .peek(resourceCreated -> assertNotNull(resourceCreated.resourceId()))
            .map(ResourceCreated::workflowStepName)
            .collect(Collectors.toList());
        for (String expectedName : expectedStepNames) {
            assertTrue(workflowStepNames.contains(expectedName));
        }

        // This template should create 5 resources, connector_id, registered model_id, deployed model_id and pipelineId
        assertEquals(5, resourcesCreated.size());
        String modelId = resourcesCreated.get(2).resourceId();

        GetPipelineResponse getPipelinesResponse = getPipelines("append-1");

        assertTrue(getPipelinesResponse.pipelines().get(0).toString().contains(modelId));

    }

    public void testDefaultCohereUseCase() throws Exception {
        // Hit Create Workflow API with original template
        Response response = createWorkflowWithUseCaseWithNoValidation(
            client(),
            "cohere_embedding_model_deploy",
            List.of(CREATE_CONNECTOR_CREDENTIAL_KEY)
        );
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        response = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 30);

        List<String> expectedStepNames = List.of("create_connector", "register_remote_model", "deploy_model");

        List<String> workflowStepNames = resourcesCreated.stream()
            .peek(resourceCreated -> assertNotNull(resourceCreated.resourceId()))
            .map(ResourceCreated::workflowStepName)
            .collect(Collectors.toList());
        for (String expectedName : expectedStepNames) {
            assertTrue(workflowStepNames.contains(expectedName));
        }

        // This template should create 5 resources, connector_id, registered model_id, deployed model_id and pipelineId
        assertEquals(3, resourcesCreated.size());
    }

    public void testDefaultSemanticSearchUseCaseWithFailureExpected() throws Exception {
        // Hit Create Workflow API with original template without required params
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> createWorkflowWithUseCaseWithNoValidation(client(), "semantic_search", Collections.emptyList())
        );
        assertTrue(
            exception.getMessage()
                .contains("Missing the following required parameters for use case [semantic_search] : [create_ingest_pipeline.model_id]")
        );

        // Pass in required params
        Response response = createWorkflowWithUseCaseWithNoValidation(
            client(),
            "semantic_search",
            List.of(CREATE_INGEST_PIPELINE_MODEL_ID)
        );
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        response = provisionWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.FAILED, ProvisioningProgress.FAILED);
    }

    public void testAllDefaultUseCasesCreation() throws Exception {
        Set<String> allUseCaseNames = EnumSet.allOf(DefaultUseCases.class)
            .stream()
            .map(DefaultUseCases::getUseCaseName)
            .collect(Collectors.toSet());

        for (String useCaseName : allUseCaseNames) {
            Response response = createWorkflowWithUseCaseWithNoValidation(
                client(),
                useCaseName,
                DefaultUseCases.getRequiredParamsByUseCaseName(useCaseName)
            );
            assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

            Map<String, Object> responseMap = entityAsMap(response);
            String workflowId = (String) responseMap.get(WORKFLOW_ID);
            getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);
        }
    }

    public void testSemanticSearchWithLocalModelEndToEnd() throws Exception {
        // Checking if plugins are part of the integration test cluster so we can continue with this test
        List<String> plugins = catPlugins();
        if (!plugins.contains("opensearch-knn") && !plugins.contains("opensearch-neural-search")) {
            return;
        }
        Map<String, Object> defaults = new HashMap<>();
        defaults.put("register_local_pretrained_model.name", "huggingface/sentence-transformers/all-MiniLM-L6-v2");
        defaults.put("register_local_pretrained_model.version", "1.0.1");
        defaults.put("text_embedding.field_map.output.dimension", 384);

        Response response = createAndProvisionWorkflowWithUseCaseWithContent(client(), "semantic_search_with_local_model", defaults);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 45);

        // This template should create 4 resources, registered model_id, deployed model_id, ingest pipeline, and index name
        assertEquals(4, resourcesCreated.size());
        String modelId = resourcesCreated.get(1).resourceId();
        String pipelineId = resourcesCreated.get(2).resourceId();
        String indexName = resourcesCreated.get(3).resourceId();

        // Short wait before ingesting data
        Thread.sleep(30000);

        String docContent = "{\"passage_text\": \"Hello planet\"\n}";
        ingestSingleDoc(docContent, indexName);
        // Short wait before neural search
        Thread.sleep(3000);
        SearchResponse neuralSearchResponse = neuralSearchRequest(indexName, modelId);
        assertNotNull(neuralSearchResponse);
        Thread.sleep(500);

        // Hit Deprovision API using allow_delete but only for the pipeline
        Response deprovisionResponse = null;
        try {
            // By design, this may not completely deprovision the first time if it takes >2s to process removals
            deprovisionResponse = deprovisionWorkflowWithAllowDelete(client(), workflowId, pipelineId);
            assertBusy(
                () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
                30,
                TimeUnit.SECONDS
            );
        } catch (ResponseException re) {
            // 403 return if completed with only index remaining to delete
            assertEquals(RestStatus.FORBIDDEN, TestHelpers.restStatus(re.getResponse()));
        } catch (ComparisonFailure e) {
            // 202 return if still processing
            assertNotNull(deprovisionResponse);
            assertEquals(RestStatus.ACCEPTED, TestHelpers.restStatus(deprovisionResponse));
        }
        if (deprovisionResponse != null && TestHelpers.restStatus(deprovisionResponse) == RestStatus.ACCEPTED) {
            // Short wait before we try again
            Thread.sleep(10000);
            // Expected failure since we haven't provided allow_delete param
            try {
                deprovisionResponse = deprovisionWorkflowWithAllowDelete(client(), workflowId, pipelineId);
            } catch (ResponseException re) {
                // Expected 403 return with only index remaining to delete
                assertEquals(RestStatus.FORBIDDEN, TestHelpers.restStatus(re.getResponse()));
            }
        }
        // Now try again with allow_delete for the index
        deprovisionResponse = deprovisionWorkflowWithAllowDelete(client(), workflowId, pipelineId + "," + indexName);
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED); },
            30,
            TimeUnit.SECONDS
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprovisionResponse));

        // Hit Delete API
        Response deleteResponse = deleteWorkflow(client(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteResponse));
    }
}
