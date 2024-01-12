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
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.FlowFrameworkRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.junit.After;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkSecureRestApiIT extends FlowFrameworkRestTestCase {

    @After
    public void tearDownSecureTests() throws IOException {
        IOUtils.close(fullAccessClient(), readAccessClient());
        deleteUser(FULL_ACCESS_USER);
        deleteUser(READ_ACCESS_USER);
    }

    public void testCreateWorkflowWithReadAccess() throws Exception {
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        ResponseException exception = expectThrows(ResponseException.class, () -> createWorkflow(readAccessClient(), template));
        assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opensearch/flow_framework/workflow/create]"));
    }

    public void testProvisionWorkflowWithReadAccess() throws Exception {
        ResponseException exception = expectThrows(ResponseException.class, () -> provisionWorkflow(readAccessClient(), "test"));
        assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opensearch/flow_framework/workflow/provision]"));
    }

    public void testDeleteWorkflowWithReadAccess() throws Exception {
        ResponseException exception = expectThrows(ResponseException.class, () -> deleteWorkflow(readAccessClient(), "test"));
        assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opensearch/flow_framework/workflow/delete]"));
    }

    public void testDeprovisionWorkflowWithReadAcess() throws Exception {
        ResponseException exception = expectThrows(ResponseException.class, () -> deprovisionWorkflow(readAccessClient(), "test"));
        assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/opensearch/flow_framework/workflow/deprovision]"));
    }

    public void testGetWorkflowStepsWithReadAccess() throws Exception {
        Response response = getWorkflowStep(readAccessClient());
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testGetWorkflowWithReadAccess() throws Exception {
        // No permissions to create, so we assert only that the response status isnt forbidden
        ResponseException exception = expectThrows(ResponseException.class, () -> getWorkflow(readAccessClient(), "test"));
        assertEquals(RestStatus.NOT_FOUND, TestHelpers.restStatus(exception.getResponse()));
    }

    public void testSearchWorkflowWithReadAccess() throws Exception {
        // Use full access client to invoke create workflow to ensure the template/state indices are created
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(fullAccessClient(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // No permissions to create, so we assert only that the response status isnt forbidden
        String termIdQuery = "{\"query\":{\"ids\":{\"values\":[\"test\"]}}}";
        SearchResponse seachResponse = searchWorkflows(readAccessClient(), termIdQuery);
        assertEquals(RestStatus.OK, seachResponse.status());
    }

    public void testGetWorkflowStateWithReadAccess() throws Exception {
        // Use the full access client to invoke create workflow to ensure the template/state indices are created
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(fullAccessClient(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // No permissions to create or provision, so we assert only that the response status isnt forbidden
        ResponseException exception = expectThrows(ResponseException.class, () -> getWorkflowStatus(readAccessClient(), "test", false));
        assertTrue(exception.getMessage().contains("Fail to find workflow"));
        assertEquals(RestStatus.NOT_FOUND, TestHelpers.restStatus(exception.getResponse()));
    }

    public void testSearchWorkflowStateWithReadAccess() throws Exception {
        // Use the full access client to invoke create workflow to ensure the template/state indices are created
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(fullAccessClient(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // No permissions to create, so we assert only that the response status isnt forbidden
        String termIdQuery = "{\"query\":{\"ids\":{\"values\":[\"test\"]}}}";
        SearchResponse searchResponse = searchWorkflowState(readAccessClient(), termIdQuery);
        assertEquals(RestStatus.OK, searchResponse.status());
    }

    public void testCreateProvisionDeprovisionWorkflowWithFullAccess() throws Exception {
        // Invoke create workflow API
        Template template = TestHelpers.createTemplateFromFile("createconnector-registerremotemodel-deploymodel.json");
        Response response = createWorkflow(fullAccessClient(), template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(response));

        // Retrieve workflow ID
        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);

        // Invoke search workflows API
        String termIdQuery = "{\"query\":{\"ids\":{\"values\":[\"" + workflowId + "\"]}}}";
        SearchResponse searchResponse = searchWorkflows(fullAccessClient(), termIdQuery);
        assertEquals(RestStatus.OK, searchResponse.status());

        // Invoke provision API
        response = provisionWorkflow(fullAccessClient(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        // Invoke status API
        response = getWorkflowStatus(fullAccessClient(), workflowId, false);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        // Invoke deprovision API
        response = deprovisionWorkflow(fullAccessClient(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        // Invoke delete API
        response = deleteWorkflow(fullAccessClient(), workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    public void testGetWorkflowStepWithFullAccess() throws Exception {
        Response response = getWorkflowStep(fullAccessClient());
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

}
