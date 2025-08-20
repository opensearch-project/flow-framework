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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.FlowFrameworkTenantAwareRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.Workflow;
import org.opensearch.flowframework.model.WorkflowNode;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

public class RestWorkflowProvisionTenantAwareIT extends FlowFrameworkTenantAwareRestTestCase {

    private static final String WORKFLOW_PATH = WORKFLOW_URI + "/";
    private static final String PROVISION = "/_provision";
    private static final String DEPROVISION = "/_deprovision";
    private static final String STATUS_ALL = "/_status?all=true";

    public void testWorkflowProvisionThrottle() throws Exception {
        boolean multiTenancyEnabled = isMultiTenancyEnabled();
        // Code assumes indices exist with multitenancy
        if (multiTenancyEnabled) {
            createMLCommonsIndices();
            createFlowFrameworkIndices();
        }

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
        assertOkOrAccepted(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(workflowId1, map.get(WORKFLOW_ID).toString());
        // During the 3 second async provisioning, try another one
        // Another workflow should be fine
        response = makeRequest(otherTenantRequest, POST, WORKFLOW_PATH + otherWorkflowId + PROVISION);
        assertOkOrAccepted(response);
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
            assertOkOrAccepted(response);
            map = responseToMap(response);
            assertTrue(map.containsKey(WORKFLOW_ID));
            assertEquals(workflowId2, map.get(WORKFLOW_ID).toString());
        } else {
            // No throttling at all without multitenancy
            response = makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId2 + PROVISION);
            assertOkOrAccepted(response);
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
        assertOkOrAccepted(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(workflowId1, map.get(WORKFLOW_ID).toString());

        // During the 3 second async reprovisioning, try another one
        // Another workflow should be fine
        otherWorkflowRequest = getRestRequestWithHeadersAndContent(otherTenantId, createNoOpTemplateWithDelayNodes(2));
        response = makeRequest(otherWorkflowRequest, PUT, WORKFLOW_PATH + otherWorkflowId + "?reprovision=true");
        assertOkOrAccepted(response);
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
            assertOkOrAccepted(response);
            map = responseToMap(response);
            assertTrue(map.containsKey(WORKFLOW_ID));
            assertEquals(workflowId2, map.get(WORKFLOW_ID).toString());
        } else {
            // No throttling at all without multitenancy
            response = makeRequest(updateWorkflowRequest, PUT, WORKFLOW_PATH + workflowId2 + "?reprovision=true");
            assertOkOrAccepted(response);
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
        // Deprovision API is synchronous so we have to multithread.
        // Since no-op template doesn't have resources, deprovisioning is mostly a state index update
        // So we hack in some fake resources to generate 3s of delay time
        String fakeResources = createFakeResources();
        for (String workflowId : new String[] { workflowId1, workflowId2, otherWorkflowId }) {
            response = TestHelpers.makeRequest(
                client(),
                "POST",
                CommonValue.WORKFLOW_STATE_INDEX + "/_update/" + workflowId,
                null,
                fakeResources,
                List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
            );
            assertOK(response);
        }

        // Create our own thread pool for the test so we can shut it down cleanly when done
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<Response> future1 = executor.submit(() -> makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId1 + DEPROVISION));
            Future<Response> otherFuture = executor.submit(
                () -> makeRequest(otherTenantRequest, POST, WORKFLOW_PATH + otherWorkflowId + DEPROVISION)
            );
            Future<Response> future2 = executor.submit(() -> makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId2 + DEPROVISION));

            // Wait for all futures to complete
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            // otherfuture should be successful always.
            // Expect 200, may be 202
            assertOkOrAccepted(otherFuture.get());

            // in multitenancy, one of future1/future2 should be OK, the other is throttled
            // otherwise both should be successful
            int successCount = 0;
            int throttledCount = 0;

            for (Future<Response> future : Arrays.asList(future1, future2)) {
                try {
                    Response r = future.get();
                    assertOkOrAccepted(r);
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
            }

            if (multiTenancyEnabled) {
                assertEquals(1, successCount);
                assertEquals(1, throttledCount);
            } else {
                assertEquals(2, successCount);
                assertEquals(0, throttledCount);
            }
        } finally {
            // Shut down even if an exception occurs
            executor.shutdownNow();
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
            nodes.add(new WorkflowNode("no-op-" + i, "noop", Collections.emptyMap(), Map.of("delay", "3s")));
        }
        Workflow provisionWorkflow = new Workflow(Collections.emptyMap(), nodes, Collections.emptyList());
        return Template.builder().name("noop").workflows(Map.of("provision", provisionWorkflow)).build().toJson();
    }

    private static String createFakeResources() throws IOException {
        // Generate some reindex resources which use NoOpStep for deprovisioning
        // 100ms delay between steps so using 30 steps for a 3s delay
        String resourceFormat =
            "{\"workflow_step_name\":\"reindex\",\"workflow_step_id\":\"reindex_%d\",\"resource_type\":\"index\",\"resource_id\":\"FakeIndex\"}";
        String fakeResources = IntStream.rangeClosed(1, 30)
            .mapToObj(i -> String.format(Locale.ROOT, resourceFormat, i))
            .collect(Collectors.joining(",", "[", "]"));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("script").field("source", "ctx._source.resources_created = params.resources");
        builder.startObject("params")
            .field(
                "resources",
                JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, fakeResources)
                    .list()
            );
        builder.endObject();
        builder.endObject();
        builder.endObject();
        return builder.toString();
    }
}
