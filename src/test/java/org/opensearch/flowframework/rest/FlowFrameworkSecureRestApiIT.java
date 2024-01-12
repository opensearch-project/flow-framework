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
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.FlowFrameworkRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.junit.After;

import java.io.IOException;

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
        assertTrue(exception.getMessage().contains("There are no templates in the global_context"));
        assertEquals(RestStatus.NOT_FOUND, TestHelpers.restStatus(exception.getResponse()));
    }

    public void testSearchWorkflowWithReadAccess() throws Exception {
        // No permissions to create, so we assert only that the response status isnt forbidden
        String termIdQuery = "{\"query\":{\"ids\":{\"values\":[\"test\"]}}}";
        ResponseException exception = expectThrows(ResponseException.class, () -> searchWorkflows(readAccessClient(), termIdQuery));
        assertTrue(exception.getMessage().contains("no such index [.plugins-flow-framework-templates]"));
        assertEquals(RestStatus.NOT_FOUND, TestHelpers.restStatus(exception.getResponse()));
    }

    public void testGetWorkflowStateWithReadAccess() throws Exception {
        // No permissions to create or provision, so we assert only that the response status isnt forbidden
        ResponseException exception = expectThrows(ResponseException.class, () -> getWorkflowStatus(readAccessClient(), "test", false));
        assertTrue(exception.getMessage().contains("Fail to find workflow"));
        assertEquals(RestStatus.NOT_FOUND, TestHelpers.restStatus(exception.getResponse()));
    }

    public void testSearchWorkflowStateWithReadAccess() throws Exception {
        // No permissions to create, so we assert only that the response status isnt forbidden
        String termIdQuery = "{\"query\":{\"ids\":{\"values\":[\"test\"]}}}";
        ResponseException exception = expectThrows(ResponseException.class, () -> searchWorkflowState(readAccessClient(), termIdQuery));
        assertTrue(exception.getMessage().contains("no such index [.plugins-flow-framework-state]"));
        assertEquals(RestStatus.NOT_FOUND, TestHelpers.restStatus(exception.getResponse()));
    }

}
