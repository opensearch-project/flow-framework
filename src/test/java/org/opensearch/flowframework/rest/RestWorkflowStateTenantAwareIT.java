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

public class RestWorkflowStateTenantAwareIT extends FlowFrameworkTenantAwareRestTestCase {

    private static final String WORKFLOW_PATH = WORKFLOW_URI + "/";
    private static final String WORKFLOW_STATE_PATH = WORKFLOW_URI + "/state/";
    private static final String PROVISION = "/_provision";
    private static final String DEPROVISION = "/_deprovision";
    private static final String STATUS_ALL = "/_status?all=true";
    private static final String CLEAR_STATUS = "?clear_status=true";

    public void testWorkflowStateCRUD() throws Exception {
        boolean multiTenancyEnabled = isMultiTenancyEnabled();

        /*
         * Create
         */
        // Create a workflow with a tenant id
        RestRequest createWorkflowRequest = getRestRequestWithHeadersAndContent(tenantId, createRemoteModelTemplate());
        Response response = makeRequest(createWorkflowRequest, POST, WORKFLOW_PATH);
        assertOK(response);
        Map<String, Object> map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        String workflowId = map.get(WORKFLOW_ID).toString();

        /*
         * Get
         */
        // Now try to get that workflow's state
        response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("NOT_STARTED", map.get("state"));
        if (multiTenancyEnabled) {
            assertEquals(tenantId, map.get(TENANT_ID_FIELD));
        } else {
            assertNull(map.get(TENANT_ID_FIELD));
        }

        // Now try again with an other ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL)
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
            response = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("NOT_STARTED", map.get("state"));
        }

        // Now try again with a null ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(nullTenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertForbidden(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        } else {
            response = makeRequest(nullTenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("NOT_STARTED", map.get("state"));
        }

        /*
         * Provision
         */
        // Try to provision with the wrong tenant id
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(otherTenantRequest, POST, WORKFLOW_PATH + workflowId + PROVISION)
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
        }

        // Verify state still not started
        response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("NOT_STARTED", map.get("state"));

        // Now try again with a null ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(nullTenantRequest, POST, WORKFLOW_PATH + workflowId + PROVISION)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertForbidden(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        }

        // Verify state still not started
        response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("NOT_STARTED", map.get("state"));

        // Now finally provision the right way
        response = makeRequest(tenantRequest, POST, WORKFLOW_PATH + workflowId + PROVISION);
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(workflowId, map.get(WORKFLOW_ID).toString());

        assertBusy(() -> {
            // Verify state completed
            Response restResponse = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
            assertOK(restResponse);
            Map<String, Object> stateMap = responseToMap(restResponse);
            assertEquals("COMPLETED", stateMap.get("state"));
        }, 20, TimeUnit.SECONDS);

        /*
         * Search
         */
        // Create and provision second workflow using otherTenantId
        RestRequest otherWorkflowRequest = getRestRequestWithHeadersAndContent(otherTenantId, createRemoteModelTemplate());
        response = makeRequest(otherWorkflowRequest, POST, WORKFLOW_URI + "?provision=true");
        assertOK(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        String otherWorkflowId = map.get(WORKFLOW_ID).toString();

        assertBusy(() -> {
            // Verify state completed
            Response restResponse = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId + STATUS_ALL);
            assertOK(restResponse);
            Map<String, Object> stateMap = responseToMap(restResponse);
            assertEquals("COMPLETED", stateMap.get("state"));
        }, 20, TimeUnit.SECONDS);

        // Retry these tests until they pass. Search requires refresh, can take 15s on DDB
        refreshAllIndices();

        assertBusy(() -> {
            // Search should show only the workflow state for tenant
            Response restResponse = makeRequest(tenantMatchAllRequest, GET, WORKFLOW_STATE_PATH + "_search");
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
            Response restResponse = makeRequest(otherTenantMatchAllRequest, GET, WORKFLOW_STATE_PATH + "_search");
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
                () -> makeRequest(nullTenantMatchAllRequest, GET, WORKFLOW_STATE_PATH + "_search")
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
        // Deleting template without state prevents deleting the state later. Working around this with tenant id checks elsewhere is
        // possible but complex and better handled after resolving https://github.com/opensearch-project/flow-framework/issues/986
        response = makeRequest(tenantRequest, DELETE, WORKFLOW_PATH + workflowId + (multiTenancyEnabled ? CLEAR_STATUS : ""));
        assertOK(response);
        map = responseToMap(response);
        assertEquals(workflowId, map.get(DOC_ID).toString());

        // Verify the deletion
        ResponseException ex = assertThrows(ResponseException.class, () -> makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId));
        response = ex.getResponse();
        assertNotFound(response);
        map = responseToMap(response);
        assertEquals("Failed to retrieve template (" + workflowId + ") from global context.", getErrorReasonFromResponseMap(map));

        if (!multiTenancyEnabled) {
            // Verify state still exists
            response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("COMPLETED", map.get("state"));

            // Now delete with clear status
            response = makeRequest(tenantRequest, DELETE, WORKFLOW_PATH + workflowId + CLEAR_STATUS);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("not_found", map.get("result"));
        }

        // Verify state deleted
        ex = assertThrows(ResponseException.class, () -> makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL));
        response = ex.getResponse();
        assertNotFound(response);

        /*
         * Deprovision
         */
        // Try to deprovision with the wrong tenant id
        if (multiTenancyEnabled) {
            ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(tenantRequest, POST, WORKFLOW_PATH + otherWorkflowId + DEPROVISION)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals(
                    "Failed to retrieve template (" + otherWorkflowId + ") from global context.",
                    getErrorReasonFromResponseMap(map)
                );
            } else {
                assertForbidden(response);
                assertEquals(NO_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }
        }

        // Verify state still completed
        response = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId + STATUS_ALL);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("COMPLETED", map.get("state"));

        // Now try again with a null ID
        if (multiTenancyEnabled) {
            ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(nullTenantRequest, POST, WORKFLOW_PATH + otherWorkflowId + DEPROVISION)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertForbidden(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        }

        // Verify state still completed
        response = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId + STATUS_ALL);
        assertOK(response);
        map = responseToMap(response);
        assertEquals("COMPLETED", map.get("state"));

        // Now finally deprovision the right way
        response = makeRequest(otherTenantRequest, POST, WORKFLOW_PATH + otherWorkflowId + DEPROVISION);
        // Expect 200, may be 202
        assertOkOrAccepted(response);
        map = responseToMap(response);
        assertTrue(map.containsKey(WORKFLOW_ID));
        assertEquals(otherWorkflowId, map.get(WORKFLOW_ID).toString());

        assertBusy(() -> {
            // Verify state not started
            Response restResponse = makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId + STATUS_ALL);
            assertOK(restResponse);
            Map<String, Object> stateMap = responseToMap(restResponse);
            assertEquals("NOT_STARTED", stateMap.get("state"));
        }, 20, TimeUnit.SECONDS);

        // Delete workflow from tenant without specifying to delete state
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

        // Verify state deleted
        ex = assertThrows(
            ResponseException.class,
            () -> makeRequest(otherTenantRequest, GET, WORKFLOW_PATH + otherWorkflowId + STATUS_ALL)
        );
        response = ex.getResponse();
        assertNotFound(response);
    }

    private static String createRemoteModelTemplate() throws IOException {
        return ParseUtils.resourceToString("/template/createconnector-registerremotemodel-deploymodel.json");
    }
}
