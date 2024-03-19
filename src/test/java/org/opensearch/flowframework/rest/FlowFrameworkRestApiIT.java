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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ingest.GetPipelineResponse;
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
import org.junit.Before;
import org.junit.ComparisonFailure;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkRestApiIT extends FlowFrameworkRestTestCase {
    private static final Logger logger = LogManager.getLogger(FlowFrameworkRestApiIT.class);

    private static AtomicBoolean waitToStart = new AtomicBoolean(true);

    @Before
    public void waitToStart() throws Exception {
        // ML Commons cron job runs every 10 seconds and takes 20+ seconds to initialize .plugins-ml-config index
        // Delay on the first attempt for 25 seconds to allow this initialization and prevent flaky tests
        if (waitToStart.getAndSet(false)) {
            CountDownLatch latch = new CountDownLatch(1);
            latch.await(25, TimeUnit.SECONDS);
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

        // Ensure Ml config index is initialized as creating a connector requires this, then hit Provision API and assert status
        Response provisionResponse;
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
            provisionResponse = provisionWorkflow(client(), workflowId);
        } else {
            provisionResponse = provisionWorkflow(client(), workflowId);
        }
        assertEquals(RestStatus.OK, TestHelpers.restStatus(provisionResponse));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Failed update since provisioning has started
        ResponseException exceptionProvisioned = expectThrows(
            ResponseException.class,
            () -> updateWorkflow(client(), workflowId, template)
        );
        assertTrue(exceptionProvisioned.getMessage().contains("The template has already been provisioned so it can't be updated"));

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
        Template templateWithMissingInputs = new Template.Builder(template).workflows(Map.of(PROVISION_WORKFLOW, missingInputs)).build();

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

        Template cyclicalTemplate = new Template.Builder(template).workflows(Map.of(PROVISION_WORKFLOW, cyclicalWorkflow)).build();

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

        // Ensure Ml config index is initialized as creating a connector requires this, then hit Provision API and assert status
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
            response = provisionWorkflow(client(), workflowId);
        } else {
            response = provisionWorkflow(client(), workflowId);
        }

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
    }

    public void testCreateAndProvisionAgentFrameworkWorkflow() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("agent-framework.json");

        // Hit Create Workflow API to create agent-framework template, with template validation check and provision parameter
        Response response;
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
            response = createWorkflowWithProvision(client(), template);
        } else {
            response = createWorkflowWithProvision(client(), template);
        }
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

        // Ensure Ml config index is initialized as creating a connector requires this, then hit Provision API and assert status
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
            response = provisionWorkflow(client(), workflowId);
        } else {
            response = provisionWorkflow(client(), workflowId);
        }

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

        List workflowStepNames = resourcesCreated.stream()
            .peek(resourceCreated -> assertNotNull(resourceCreated.resourceId()))
            .map(ResourceCreated::workflowStepName)
            .collect(Collectors.toList());
        for (String expectedName : expectedStepNames) {
            assertTrue(workflowStepNames.contains(expectedName));
        }

        // This template should create 5 resources, connector_id, registered model_id, deployed model_id and pipelineId
        assertEquals(5, resourcesCreated.size());
        String modelId = resourcesCreated.get(2).resourceId();

        GetPipelineResponse getPipelinesResponse = getPipelines();

        assertTrue(getPipelinesResponse.pipelines().get(0).toString().contains(modelId));

    }

    public void testDefaultCohereUseCase() throws Exception {

        // Hit Create Workflow API with original template
        Response response = createWorkflowWithUseCase(client(), "cohere-embedding_model_deploy");
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // Ensure Ml config index is initialized as creating a connector requires this, then hit Provision API and assert status
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
            response = provisionWorkflow(client(), workflowId);
        } else {
            response = provisionWorkflow(client(), workflowId);
        }

        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.PROVISIONING, ProvisioningProgress.IN_PROGRESS);

        // Wait until provisioning has completed successfully before attempting to retrieve created resources
        List<ResourceCreated> resourcesCreated = getResourcesCreated(client(), workflowId, 30);

        List<String> expectedStepNames = List.of("create_connector", "register_remote_model", "deploy_model");

        List workflowStepNames = resourcesCreated.stream()
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

        // Hit Create Workflow API with original template
        Response response = createWorkflowWithUseCase(client(), "semantic_search");
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        getAndAssertWorkflowStatus(client(), workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        // Ensure Ml config index is initialized as creating a connector requires this, then hit Provision API and assert status
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
            response = provisionWorkflow(client(), workflowId);
        } else {
            response = provisionWorkflow(client(), workflowId);
        }

        // expecting a failure since there is no neural-search plugin in cluster to provide text-embedding processor
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
        getAndAssertWorkflowStatus(client(), workflowId, State.FAILED, ProvisioningProgress.FAILED);

        String error = getAndWorkflowStatusError(client(), workflowId);
        assertTrue(
            error.contains(
                "org.opensearch.flowframework.exception.WorkflowStepException during step create_ingest_pipeline, restStatus: BAD_REQUEST"
            )
        );

    }

}
