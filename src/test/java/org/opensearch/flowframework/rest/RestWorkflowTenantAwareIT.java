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
import org.opensearch.flowframework.FlowFrameworkTenantAwareRestTestCase;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.flowframework.common.CommonValue.TENANT_ID_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

public class RestWorkflowTenantAwareIT extends FlowFrameworkTenantAwareRestTestCase {

    private static final String WORKFLOW_PATH = WORKFLOW_URI + "/";

    public void testWorkflowCRUD() throws Exception {
        boolean multiTenancyEnabled = isMultiTenancyEnabled();

        /*
         * Create
         */
        // Create a workflow with a tenant id
        RestRequest createWorkflowRequest = getRestRequestWithHeadersAndContent(tenantId, createNoOpTemplate());
        Response response = makeRequest(createWorkflowRequest, POST, WORKFLOW_PATH);
        assertOK(response);
        Map<String, Object> map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        String workflowId = map.get(WORKFLOW_ID).toString();

        /*
         * Get
         */
        // Now try to get that workflow
        response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("noop", map.get("name"));
        if (multiTenancyEnabled) {
            assertEquals(tenantId, map.get(TENANT_ID_FIELD));
        } else {
            assertNull(map.get(TENANT_ID_FIELD));
        }

        // Now try again with an other ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + workflowId)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals("Failed to retrieve template (" + workflowId + ") from global context.", getErrorReasonFromResponseMap(map));
            } else {
                assertForbidden(response);
                assertEquals(NO_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }
        } else {
            response = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + workflowId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("noop", map.get("name"));
        }

        // Now try again with a null ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(nullTenantRequest, GET, WORKFLOW_PATH + workflowId)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertForbidden(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        } else {
            response = makeRequest(nullTenantRequest, GET, WORKFLOW_PATH + workflowId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("noop", map.get("name"));
        }

        /*
         * Update
         */
        // Now attempt to update the workflow name
        RestRequest updateRequest = getRestRequestWithHeadersAndContent(tenantId, "{\"name\":\"Updated name\"}");
        response = makeRequest(updateRequest, PUT, WORKFLOW_PATH + workflowId);
        assertOK(response);
        map = responseToMap(response);
        assertEquals(workflowId, map.get(WORKFLOW_ID).toString());

        // Verify the update
        response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("Updated name", map.get("name"));

        // Try the update with other tenant ID
        RestRequest otherUpdateRequest = getRestRequestWithHeadersAndContent(otherTenantId, "{\"name\":\"Other tenant name\"}");
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(otherUpdateRequest, PUT, WORKFLOW_PATH + workflowId)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals("Failed to retrieve template (" + workflowId + ") from global context.", getErrorReasonFromResponseMap(map));
            } else {
                assertForbidden(response);
                assertEquals(NO_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }
        } else {
            response = makeRequest(otherUpdateRequest, PUT, WORKFLOW_PATH + workflowId);
            assertOK(response);
            // Verify the update
            response = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + workflowId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("Other tenant name", map.get("name"));
        }

        // Try the update with no tenant ID
        RestRequest nullUpdateRequest = getRestRequestWithHeadersAndContent(null, "{\"name\":\"Null tenant name\"}");
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(nullUpdateRequest, PUT, WORKFLOW_PATH + workflowId)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertForbidden(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        } else {
            response = makeRequest(nullUpdateRequest, PUT, WORKFLOW_PATH + workflowId);
            assertOK(response);
            // Verify the update
            response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("Null tenant name", map.get("name"));
        }

        // Verify no change from original update when multiTenancy enabled
        if (multiTenancyEnabled) {
            response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("Updated name", map.get("name"));
        }

        /*
         * Search
         */
        // Create a second workflow using otherTenantId
        RestRequest otherWorkflowRequest = getRestRequestWithHeadersAndContent(otherTenantId, createNoOpTemplate());
        response = makeRequest(otherWorkflowRequest, POST, WORKFLOW_PATH);
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        String otherWorkflowId = map.get(WORKFLOW_ID).toString();

        // Verify it
        response = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("noop", map.get("name"));

        // Retry these tests until they pass. Search requires refresh, can take 15s on DDB
        refreshAllIndices();

        assertBusy(() -> {
            // Search should show only the workflow for tenant
            Response restResponse = makeRequest(tenantMatchAllRequest, GET, WORKFLOW_PATH + "_search");
            assertOK(restResponse);
            SearchResponse searchResponse = searchResponseFromResponse(restResponse);
            if (multiTenancyEnabled) {
                assertEquals(1, searchResponse.getHits().getTotalHits().value);
                assertEquals(tenantId, searchResponse.getHits().getHits()[0].getSourceAsMap().get(TENANT_ID_FIELD));
            } else {
                assertEquals(2, searchResponse.getHits().getTotalHits().value);
                assertNull(searchResponse.getHits().getHits()[0].getSourceAsMap().get(TENANT_ID_FIELD));
                assertNull(searchResponse.getHits().getHits()[1].getSourceAsMap().get(TENANT_ID_FIELD));
            }
        }, 20, TimeUnit.SECONDS);

        assertBusy(() -> {
            // Search should show only the workflow for other tenant
            Response restResponse = makeRequest(otherTenantMatchAllRequest, GET, WORKFLOW_PATH + "_search");
            assertOK(restResponse);
            SearchResponse searchResponse = searchResponseFromResponse(restResponse);
            if (multiTenancyEnabled) {
                assertEquals(1, searchResponse.getHits().getTotalHits().value);
                assertEquals(otherTenantId, searchResponse.getHits().getHits()[0].getSourceAsMap().get(TENANT_ID_FIELD));
            } else {
                assertEquals(2, searchResponse.getHits().getTotalHits().value);
                assertNull(searchResponse.getHits().getHits()[0].getSourceAsMap().get(TENANT_ID_FIELD));
                assertNull(searchResponse.getHits().getHits()[1].getSourceAsMap().get(TENANT_ID_FIELD));
            }
        }, 20, TimeUnit.SECONDS);

        // Search should fail without a tenant id
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(nullTenantMatchAllRequest, GET, WORKFLOW_PATH + "_search")
            );
            response = ex.getResponse();
            assertForbidden(response);
            map = responseToMap(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        } else {
            response = makeRequest(nullTenantMatchAllRequest, GET, WORKFLOW_PATH + "_search");
            assertOK(response);
            SearchResponse searchResponse = searchResponseFromResponse(response);
            assertEquals(2, searchResponse.getHits().getTotalHits().value);
            assertNull(searchResponse.getHits().getHits()[0].getSourceAsMap().get(TENANT_ID_FIELD));
            assertNull(searchResponse.getHits().getHits()[1].getSourceAsMap().get(TENANT_ID_FIELD));
        }

        /*
         * Delete
         */
        // Delete the workflows
        // First test that we can't delete other tenant workflows
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(tenantRequest, DELETE, WORKFLOW_PATH + otherWorkflowId)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals(
                    "Failed to find workflow with the provided workflow id: " + otherWorkflowId,
                    getErrorReasonFromResponseMap(map)
                );
            } else {
                assertForbidden(response);
                assertEquals(NO_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }

            ex = assertThrows(ResponseException.class, () -> makeRequest(otherTenantRequest, DELETE, WORKFLOW_PATH + workflowId));
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals("Failed to retrieve template (" + workflowId + ") from global context.", getErrorReasonFromResponseMap(map));
            } else {
                assertForbidden(response);
                assertEquals(NO_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }

            // and can't delete without a tenant ID either
            ex = assertThrows(ResponseException.class, () -> makeRequest(nullTenantRequest, DELETE, WORKFLOW_PATH + workflowId));
            response = ex.getResponse();
            map = responseToMap(response);
            assertForbidden(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        }

        // Now actually do the deletions. Same result whether multi-tenancy is enabled.
        // Delete from tenant
        response = makeRequest(tenantRequest, DELETE, WORKFLOW_PATH + workflowId);
        assertOK(response);
        map = responseToMap(response);
        assertEquals(workflowId, map.get(DOC_ID).toString());

        // Verify the deletion
        ResponseException ex = assertThrows(ResponseException.class, () -> makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId));
        response = ex.getResponse();
        assertNotFound(response);
        map = responseToMap(response);
        assertEquals("Failed to retrieve template (" + workflowId + ") from global context.", getErrorReasonFromResponseMap(map));

        // Delete from other tenant
        response = makeRequest(otherTenantRequest, DELETE, WORKFLOW_PATH + otherWorkflowId);
        assertOK(response);
        map = responseToMap(response);
        assertEquals(otherWorkflowId, map.get(DOC_ID).toString());

        // Verify the deletion
        ex = assertThrows(ResponseException.class, () -> makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId));
        response = ex.getResponse();
        assertNotFound(response);
        map = responseToMap(response);
        assertEquals("Failed to retrieve template (" + otherWorkflowId + ") from global context.", getErrorReasonFromResponseMap(map));
    }

    private static String createNoOpTemplate() throws IOException {
        return ParseUtils.resourceToString("/template/noop.json");
    }
}
