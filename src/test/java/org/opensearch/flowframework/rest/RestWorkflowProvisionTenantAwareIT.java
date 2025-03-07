/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.rest;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.flowframework.FlowFrameworkTenantAwareRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

public class RestWorkflowProvisionTenantAwareIT extends FlowFrameworkTenantAwareRestTestCase {

    private static final String WORKFLOW_PATH = WORKFLOW_URI + "/";
    private static final String PROVISION = "/_provision";
    private static final String DEPROVISION = "/_deprovision";
    private static final String STATUS_ALL = "/_status?all=true";

    public void testWorkflowProvisionThrottle() throws Exception {
        boolean multiTenancyEnabled = isMultiTenancyEnabled();

        /*
         * Setup
         */
        Response response = TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            "{\"persistent\":{\"plugins.flow_framework.max_active_provisions_per_tenant\":1,\"plugins.flow_framework.max_active_deprovisions_per_tenant\":1}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertOK(response);

        /*
        * Create
        */
        // Create a workflow with a tenant id
        RestRequest createWorkflowRequest = getRestRequestWithHeadersAndContent(tenantId, createNoOpTemplateWithDelayNodes(1));
        response = makeRequest(createWorkflowRequest, POST, WORKFLOW_PATH);
        assertOK(response);
        Map<String, Object> map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        String workflowId1 = map.get(WORKFLOW_ID).toString();

        // Create a second workflow with a tenant id
        response = makeRequest(createWorkflowRequest, POST, WORKFLOW_PATH);
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        String workflowId2 = map.get(WORKFLOW_ID).toString();

        // Create a workflow with other tenant id
        RestRequest otherWorkflowRequest = getRestRequestWithHeadersAndContent(otherTenantId, createNoOpTemplateWithDelayNodes(1));
        response = makeRequest(otherWorkflowRequest, POST, WORKFLOW_PATH);
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        String otherWorkflowId = map.get(WORKFLOW_ID).toString();

        /*
         * Provision
         */
        // Kick off a provisioning
        response = makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId1 + PROVISION);
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(workflowId1, map.get(WORKFLOW_ID).toString());
        // During the 12 second async provisioning, try another one
        // Another workflow should be fine
        response = makeRequest(otherTenantRequest, POST, WORKFLOW_PATH + otherWorkflowId + PROVISION);
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(otherWorkflowId, map.get(WORKFLOW_ID).toString());

        if (multiTenancyEnabled) {
            // Same workflow should throttle
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId2 + PROVISION)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertTooManyRequests(response);

            // Wait for workflow 1 completion
            assertBusy(() -> {
                // Verify state completed
                Response restResponse = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId1 + STATUS_ALL);
                assertOK(restResponse);
                Map<String, Object> stateMap = responseToMap(restResponse);
                assertEquals("COMPLETED", stateMap.get("state"));
            }, 20, TimeUnit.SECONDS);

            // Provision should work now
            response = makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId2 + PROVISION);
            assertOK(response);
            map = responseToMap(response);
            assertTrue(map.containsKey(WORKFLOW_ID));
            assertEquals(workflowId2, map.get(WORKFLOW_ID).toString());
        } else {
            // No throttling at all without multitenancy
            response = makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId2 + PROVISION);
            assertOK(response);
            map = responseToMap(response);
            assertTrue(map.containsKey(WORKFLOW_ID));
            assertEquals(workflowId2, map.get(WORKFLOW_ID).toString());

            // Wait for workflow 1 completion
            assertBusy(() -> {
                // Verify state completed
                Response restResponse = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId1 + STATUS_ALL);
                assertOK(restResponse);
                Map<String, Object> stateMap = responseToMap(restResponse);
                assertEquals("COMPLETED", stateMap.get("state"));
            }, 20, TimeUnit.SECONDS);
        }

        // Wait for other workflow completion
        assertBusy(() -> {
            // Verify state completed
            Response restResponse = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId + STATUS_ALL);
            assertOK(restResponse);
            Map<String, Object> stateMap = responseToMap(restResponse);
            assertEquals("COMPLETED", stateMap.get("state"));
        }, 20, TimeUnit.SECONDS);

        // Wait for workflow 2 completion
        assertBusy(() -> {
            // Verify state completed
            Response restResponse = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId2 + STATUS_ALL);
            assertOK(restResponse);
            Map<String, Object> stateMap = responseToMap(restResponse);
            assertEquals("COMPLETED", stateMap.get("state"));
        }, 20, TimeUnit.SECONDS);

        /*
         * Reprovision
         */
        // Reprovision a workflow with a tenant id
        RestRequest updateWorkflowRequest = getRestRequestWithHeadersAndContent(tenantId, createNoOpTemplateWithDelayNodes(2));
        response = makeRequest(updateWorkflowRequest, PUT, WORKFLOW_PATH + workflowId1 + "?reprovision=true");
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(workflowId1, map.get(WORKFLOW_ID).toString());

        // During the 12 second async reprovisioning, try another one
        // Another workflow should be fine
        otherWorkflowRequest = getRestRequestWithHeadersAndContent(otherTenantId, createNoOpTemplateWithDelayNodes(2));
        response = makeRequest(otherWorkflowRequest, PUT, WORKFLOW_PATH + otherWorkflowId + "?reprovision=true");
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(otherWorkflowId, map.get(WORKFLOW_ID).toString());

        if (multiTenancyEnabled) {
            // Same workflow should throttle
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(updateWorkflowRequest, PUT, WORKFLOW_PATH + workflowId2 + "?reprovision=true")
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertTooManyRequests(response);

            // Wait for workflow 1 completion
            assertBusy(() -> {
                // Verify state completed
                Response restResponse = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId1 + STATUS_ALL);
                assertOK(restResponse);
                Map<String, Object> stateMap = responseToMap(restResponse);
                assertEquals("COMPLETED", stateMap.get("state"));
            }, 20, TimeUnit.SECONDS);

            // Reprovision should work now
            response = makeRequest(updateWorkflowRequest, PUT, WORKFLOW_PATH + workflowId2 + "?reprovision=true");
            assertOK(response);
            map = responseToMap(response);
            assertTrue(map.containsKey(WORKFLOW_ID));
            assertEquals(workflowId2, map.get(WORKFLOW_ID).toString());
        } else {
            // No throttling at all without multitenancy
            response = makeRequest(updateWorkflowRequest, PUT, WORKFLOW_PATH + workflowId2 + "?reprovision=true");
            assertOK(response);
            map = responseToMap(response);
            assertTrue(map.containsKey(WORKFLOW_ID));
            assertEquals(workflowId2, map.get(WORKFLOW_ID).toString());

            // Wait for workflow 1 completion
            assertBusy(() -> {
                // Verify state completed
                Response restResponse = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId1 + STATUS_ALL);
                assertOK(restResponse);
                Map<String, Object> stateMap = responseToMap(restResponse);
                assertEquals("COMPLETED", stateMap.get("state"));
            }, 20, TimeUnit.SECONDS);
        }

        // Wait for other workflow completion
        assertBusy(() -> {
            // Verify state completed
            Response restResponse = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId + STATUS_ALL);
            assertOK(restResponse);
            Map<String, Object> stateMap = responseToMap(restResponse);
            assertEquals("COMPLETED", stateMap.get("state"));
        }, 20, TimeUnit.SECONDS);

        // Wait for workflow 2 completion
        assertBusy(() -> {
            // Verify state completed
            Response restResponse = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId2 + STATUS_ALL);
            assertOK(restResponse);
            Map<String, Object> stateMap = responseToMap(restResponse);
            assertEquals("COMPLETED", stateMap.get("state"));
        }, 20, TimeUnit.SECONDS);

        /*
         * Deprovision
         */
        // Deprovision API is synchronous so we have to multithread
        // Since no-op template doesn't have resources this really only takes as long as the state update
        CompletableFuture<Response> future1 = CompletableFuture.supplyAsync(() -> {
            try {
                return makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId1 + DEPROVISION);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
        CompletableFuture<Response> future2 = CompletableFuture.supplyAsync(() -> {
            try {
                return makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId2 + DEPROVISION);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
        CompletableFuture<Response> otherFuture = CompletableFuture.supplyAsync(() -> {
            try {
                return makeRequest(otherTenantRequest, POST, WORKFLOW_PATH + otherWorkflowId + DEPROVISION);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(future1, future2, otherFuture);
        if (multiTenancyEnabled) {
            // Wait for all futures to complete, but don't throw exceptions
            combinedFuture.handle((v, e) -> null).get(20, TimeUnit.SECONDS);
        } else {
            // Should not get any exceptions
            combinedFuture.get(20, TimeUnit.SECONDS);
        }

        // otherfuture should be successful always.
        // Expect 200, may be 202
        assertOkOrAccepted(otherFuture.get());
        map = responseToMap(otherFuture.get());
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(otherWorkflowId, map.get(WORKFLOW_ID).toString());

        // in multitenancy, at least one of future1/future2 should be OK
        // no more than one of the other is throttled
        // otherwise both should be successful
        int successCount = 0;
        int throttledCount = 0;

        try {
            Response response1 = future1.get();
            assertOkOrAccepted(response1);
            successCount++;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ResponseException) {
                ResponseException re = (ResponseException) e.getCause();
                assertTooManyRequests(re.getResponse());
                throttledCount++;
            } else {
                fail("Unexpected exception type");
            }
        }

        try {
            Response response2 = future2.get();
            assertOkOrAccepted(response2);
            successCount++;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ResponseException) {
                ResponseException re = (ResponseException) e.getCause();
                assertTooManyRequests(re.getResponse());
                throttledCount++;
            } else {
                fail("Unexpected exception type");
            }
        }

        if (multiTenancyEnabled) {
            assertTrue(successCount >= 1);
            assertTrue(throttledCount <= 1);
        } else {
            assertEquals(2, successCount);
            assertEquals(0, throttledCount);
        }

        /*
        * Delete
        */
        // Delete from tenant
        response = makeRequest(tenantRequest, DELETE, WORKFLOW_PATH + workflowId1);
        assertOK(response);
        map = responseToMap(response);
        assertEquals(workflowId1, map.get(DOC_ID).toString());

        response = makeRequest(tenantRequest, DELETE, WORKFLOW_PATH + workflowId2);
        assertOK(response);
        map = responseToMap(response);
        assertEquals(workflowId2, map.get(DOC_ID).toString());

        // Delete from other tenant
        response = makeRequest(otherTenantRequest, DELETE, WORKFLOW_PATH + otherWorkflowId);
        assertOK(response);
        map = responseToMap(response);
        assertEquals(otherWorkflowId, map.get(DOC_ID).toString());
    }

    private static String createNoOpTemplateWithDelayNodes(int numNodes) throws IOException {
        List<WorkflowNode> nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            // Fun fact: TestHelpers retries failed requests once after 10 seconds so we need a longer delay
            nodes.add(new WorkflowNode("no-op-" + i, "noop", Collections.emptyMap(), Map.of("delay", "12s")));
        }
        Workflow provisionWorkflow = new Workflow(Collections.emptyMap(), nodes, Collections.emptyList());
        return Template.builder().name("noop").workflows(Map.of("provision", provisionWorkflow)).build().toJson();
    }
}
