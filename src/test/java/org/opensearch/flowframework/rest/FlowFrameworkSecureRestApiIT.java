/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.FlowFrameworkRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.PROVISION_WORKFLOW;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkSecureRestApiIT extends FlowFrameworkRestTestCase {

    String aliceUser = "alice";
    RestClient aliceClient;
    String bobUser = "bob";
    RestClient bobClient;
    String catUser = "cat";
    RestClient catClient;
    String dogUser = "dog";
    RestClient dogClient;
    String elkUser = "elk";
    RestClient elkClient;
    String fishUser = "fish";
    RestClient fishClient;
    String goatUser = "goat";
    RestClient goatClient;
    String lionUser = "lion";
    RestClient lionClient;
    private String indexAllAccessRole = "index_all_access";
    private String indexSearchAccessRole = "index_all_search";
    private static String FLOW_FRAMEWORK_FULL_ACCESS_ROLE = "flow_framework_full_access";
    private static String ML_COMMONS_FULL_ACCESS_ROLE = "ml_full_access";
    private static String FLOW_FRAMEWORK_READ_ACCESS_ROLE = "flow_framework_read_access";

    @Before
    public void setupSecureTests() throws IOException {
        if (!isHttps()) throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        createIndexRole(indexAllAccessRole, "*");
        createSearchRole(indexSearchAccessRole, "*");
        String alicePassword = generatePassword(aliceUser);
        createUser(aliceUser, alicePassword, List.of("odfe"));
        aliceClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), aliceUser, alicePassword)
            .setSocketTimeout(60000)
            .build();

        String bobPassword = generatePassword(bobUser);
        createUser(bobUser, bobPassword, List.of("odfe"));
        bobClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), bobUser, bobPassword)
            .setSocketTimeout(60000)
            .build();

        String catPassword = generatePassword(catUser);
        createUser(catUser, catPassword, List.of("aes"));
        catClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), catUser, catPassword)
            .setSocketTimeout(60000)
            .build();

        String dogPassword = generatePassword(dogUser);
        createUser(dogUser, dogPassword, List.of());
        dogClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), dogUser, dogPassword)
            .setSocketTimeout(60000)
            .build();

        String elkPassword = generatePassword(elkUser);
        createUser(elkUser, elkPassword, List.of("odfe"));
        elkClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), elkUser, elkPassword)
            .setSocketTimeout(60000)
            .build();

        String fishPassword = generatePassword(fishUser);
        createUser(fishUser, fishPassword, List.of("odfe", "aes"));
        fishClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), fishUser, fishPassword)
            .setSocketTimeout(60000)
            .build();

        String goatPassword = generatePassword(goatUser);
        createUser(goatUser, goatPassword, List.of("opensearch"));
        goatClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), goatUser, goatPassword)
            .setSocketTimeout(60000)
            .build();

        String lionPassword = generatePassword(lionUser);
        createUser(lionUser, lionPassword, List.of("opensearch"));
        lionClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), lionUser, lionPassword)
            .setSocketTimeout(60000)
            .build();

        createRoleMapping(FLOW_FRAMEWORK_READ_ACCESS_ROLE, List.of(bobUser));
        createRoleMapping(ML_COMMONS_FULL_ACCESS_ROLE, List.of(aliceUser, catUser, dogUser, elkUser, fishUser, goatUser));
        createRoleMapping(FLOW_FRAMEWORK_FULL_ACCESS_ROLE, List.of(aliceUser, catUser, dogUser, elkUser, fishUser, goatUser));
        createRoleMapping(indexAllAccessRole, List.of(aliceUser, bobUser, catUser, dogUser, fishUser, lionUser));
        createRoleMapping(indexSearchAccessRole, List.of(goatUser));
    }

    @After
    public void tearDownSecureTests() throws IOException {
        aliceClient.close();
        bobClient.close();
        catClient.close();
        dogClient.close();
        elkClient.close();
        fishClient.close();
        goatClient.close();
        lionClient.close();
        deleteUser(aliceUser);
        deleteUser(bobUser);
        deleteUser(catUser);
        deleteUser(dogUser);
        deleteUser(elkUser);
        deleteUser(fishUser);
        deleteUser(goatUser);
        deleteUser(lionUser);
    }

    /**
     * Create an unguessable password. Simple password are weak due to https://tinyurl.com/383em9zk
     * @return a random password.
     */
    public static String generatePassword(String username) {
        String upperCase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerCase = "abcdefghijklmnopqrstuvwxyz";
        String digits = "0123456789";
        String special = "_";
        String characters = upperCase + lowerCase + digits + special;

        SecureRandom rng = new SecureRandom();

        // Ensure password includes at least one character from each set
        char[] password = new char[15];
        password[0] = upperCase.charAt(rng.nextInt(upperCase.length()));
        password[1] = lowerCase.charAt(rng.nextInt(lowerCase.length()));
        password[2] = digits.charAt(rng.nextInt(digits.length()));
        password[3] = special.charAt(rng.nextInt(special.length()));

        for (int i = 4; i < 15; i++) {
            char nextChar;
            do {
                nextChar = characters.charAt(rng.nextInt(characters.length()));
            } while (username.indexOf(nextChar) > -1);
            password[i] = nextChar;
        }

        // Shuffle the array to ensure the first 4 characters are not always in the same position
        for (int i = password.length - 1; i > 0; i--) {
            int index = rng.nextInt(i + 1);
            char temp = password[index];
            password[index] = password[i];
            password[i] = temp;
        }

        return new String(password);
    }

    public void testCreateWorkflowWithReadAccess() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        ResponseException exception = expectThrows(ResponseException.class, () -> createWorkflow(bobClient, template));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    public void testCreateWorkflowWithWriteAccess() throws Exception {
        // User Alice has FF full access, should be able to create a workflow
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response response = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));
    }

    public void testCreateWorkflowWithNoFFAccess() throws Exception {
        // User Lion has no FF access at all, should not be able to create a workflow
        disableFilterBy();
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");

        ResponseException exception = expectThrows(ResponseException.class, () -> { createWorkflow(lionClient, template); });
        assertEquals(RestStatus.FORBIDDEN.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    public void testProvisionWorkflowWithReadAccess() throws Exception {
        ResponseException exception = expectThrows(ResponseException.class, () -> provisionWorkflow(bobClient, "test"));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    public void testDeleteWorkflowWithReadAccess() throws Exception {
        ResponseException exception = expectThrows(ResponseException.class, () -> deleteWorkflow(bobClient, "test"));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    public void testDeprovisionWorkflowWithReadAcess() throws Exception {
        ResponseException exception = expectThrows(ResponseException.class, () -> deprovisionWorkflow(bobClient, "test"));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    public void testGetWorkflowStepsWithReadAccess() throws Exception {
        Response response = getWorkflowStep(bobClient);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testGetWorkflowWithReadAccess() throws Exception {
        // No permissions to create, so we assert only that the response status isnt forbidden
        ResponseException exception = expectThrows(ResponseException.class, () -> getWorkflow(bobClient, "test"));
        assertEquals(RestStatus.NOT_FOUND, TestHelpers.restStatus(exception.getResponse()));
    }

    public void testFilterByDisabled() throws Exception {
        disableFilterBy();
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response aliceWorkflow = createWorkflow(aliceClient, template);
        Map<String, Object> responseMap = entityAsMap(aliceWorkflow);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        Response response = getWorkflow(catClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testSearchWorkflowWithReadAccess() throws Exception {
        // Use full access client to invoke create workflow to ensure the template/state indices are created
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // No permissions to create, so we assert only that the response status isnt forbidden
        String termIdQuery = "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"ids\":\"test\"}}]}}}";
        SearchResponse seachResponse = searchWorkflows(bobClient, termIdQuery);
        assertEquals(RestStatus.OK, seachResponse.status());
    }

    public void testGetWorkflowStateWithReadAccess() throws Exception {
        // Use the full access client to invoke create workflow to ensure the template/state indices are created
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // No permissions to create or provision, so we assert only that the response status isnt forbidden
        Response searchResponse = getWorkflowStatus(bobClient, workflowId, false);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(searchResponse));
    }

    public void testSearchWorkflowStateWithReadAccess() throws Exception {
        // Use the full access client to invoke create workflow to ensure the template/state indices are created
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // No permissions to create, so we assert only that the response status isnt forbidden
        String termIdQuery = "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"ids\":\"test\"}}]}}}";
        SearchResponse searchResponse = searchWorkflowState(bobClient, termIdQuery);
        assertEquals(RestStatus.OK, searchResponse.status());
    }

    public void testCreateWorkflowWithNoBackendRole() throws IOException {
        enableFilterBy();
        // User Dog has FF full access, but has no backend role
        // When filter by is enabled, we block creating workflows
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Exception exception = expectThrows(IOException.class, () -> { createWorkflow(dogClient, template); });
        assertTrue(
            exception.getMessage().contains("Filter by backend roles is enabled and User dog does not have backend roles configured")
        );
    }

    public void testDeprovisionWorkflowWithWriteAccess() throws Exception {
        // User Alice has FF full access, should be able to deprovision a workflow
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response aliceWorkflow = createWorkflow(aliceClient, template);
        enableFilterBy();
        Map<String, Object> responseMap = entityAsMap(aliceWorkflow);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        Response response = deprovisionWorkflow(aliceClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testGetWorkflowWithFilterEnabled() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response aliceWorkflow = createWorkflow(aliceClient, template);
        enableFilterBy();
        Map<String, Object> responseMap = entityAsMap(aliceWorkflow);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        // User Cat has FF full access, but is part of different backend role so Cat should not be able to access alice workflow
        ResponseException exception = expectThrows(ResponseException.class, () -> getWorkflow(catClient, workflowId));
        assertTrue(exception.getMessage().contains("User does not have permissions to access workflow: " + workflowId));
    }

    public void testGetWorkflowFilterbyEnabledForAdmin() throws Exception {
        // User Alice has FF full access, should be able to create a workflow and has backend role "odfe"
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response aliceWorkflow = createWorkflow(aliceClient, template);
        enableFilterBy();
        confirmingClientIsAdmin();
        Map<String, Object> responseMap = entityAsMap(aliceWorkflow);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        Response response = getWorkflow(aliceClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testProvisionWorkflowWithWriteAccess() throws Exception {
        // User Alice has FF full access, should be able to provision a workflow
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response aliceWorkflow = createWorkflow(aliceClient, template);
        enableFilterBy();
        confirmingClientIsAdmin();
        Map<String, Object> responseMap = entityAsMap(aliceWorkflow);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        Response response = provisionWorkflow(aliceClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testDeleteWorkflowWithWriteAccess() throws Exception {
        // User Alice has FF full access, should be able to delete a workflow
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response aliceWorkflow = createWorkflow(aliceClient, template);
        enableFilterBy();
        Map<String, Object> responseMap = entityAsMap(aliceWorkflow);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        Response response = deleteWorkflow(aliceClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testCreateProvisionDeprovisionWorkflowWithFullAccess() throws Exception {
        // Invoke create workflow API
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        enableFilterBy();

        // Retrieve workflow ID
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        // Invoke search workflows API
        String termIdQuery = "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"ids\":\"" + workflowId + "\"}}]}}}";
        SearchResponse searchResponse = searchWorkflows(aliceClient, termIdQuery);
        assertEquals(RestStatus.OK, searchResponse.status());

        // Invoke provision API
        if (!indexExistsWithAdminClient(".plugins-ml-config")) {
            assertBusy(() -> assertTrue(indexExistsWithAdminClient(".plugins-ml-config")), 40, TimeUnit.SECONDS);
            response = provisionWorkflow(aliceClient, workflowId);
        } else {
            response = provisionWorkflow(aliceClient, workflowId);
        }
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        // Invoke status API
        response = getWorkflowStatus(aliceClient, workflowId, false);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        // Invoke deprovision API
        response = deprovisionWorkflow(aliceClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        // Invoke delete API
        response = deleteWorkflow(aliceClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        // Invoke status API with failure
        ResponseException exception = expectThrows(ResponseException.class, () -> getWorkflowStatus(aliceClient, workflowId, false));
        assertEquals(RestStatus.NOT_FOUND.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    public void testCreateWorkflowWithNoIndexPermission() throws Exception {
        enableFilterBy();
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        ResponseException exception = expectThrows(ResponseException.class, () -> createWorkflow(elkClient, template));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    public void testUpdateWorkflowEnabledForAdmin() throws Exception {
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

        Response response = createWorkflow(aliceClient, templateWithMissingInputs);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        ResponseException exception = expectThrows(ResponseException.class, () -> provisionWorkflow(client(), workflowId));
        assertTrue(exception.getMessage().contains("Invalid workflow, node [workflow_step_1] missing the following required inputs"));
        getAndAssertWorkflowStatus(aliceClient, workflowId, State.NOT_STARTED, ProvisioningProgress.NOT_STARTED);

        enableFilterBy();
        // User alice has admin all access, and has "odfe" backend role so client should be able to update workflow
        Response updateResponse = updateWorkflow(aliceClient, workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(updateResponse));
    }

    public void testUpdateWorkflowWithFilterEnabled() throws Exception {
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

        Response response = createWorkflow(aliceClient, templateWithMissingInputs);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        enableFilterBy();
        // User Fish has FF full access, and has "odfe" backend role which is one of Alice's backend role, so
        // Fish should be able to update workflows created by Alice. But the workflow's backend role should
        // not be replaced as Fish's backend roles.
        Response updateResponse = updateWorkflow(fishClient, workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(updateResponse));
    }

    public void testUpdateWorkflowWithNoIndexPermission() throws Exception {
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

        Response response = createWorkflow(aliceClient, templateWithMissingInputs);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        enableFilterBy();

        // User elk has FF full access, but has no read permission of index
        ResponseException exception1 = expectThrows(ResponseException.class, () -> { updateWorkflow(elkClient, workflowId, template); });
        assertEquals(RestStatus.FORBIDDEN.getStatus(), exception1.getResponse().getStatusLine().getStatusCode());
    }

    public void testGetWorkflowStepWithFullAccess() throws Exception {
        Response response = getWorkflowStep(aliceClient);
        enableFilterBy();
        confirmingClientIsAdmin();
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }
}
