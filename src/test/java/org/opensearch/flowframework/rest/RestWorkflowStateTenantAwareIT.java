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
import java.util.List;
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

    // REST paths; some subclasses need multiple of these
    private static final String AGENTS_PATH = "/_plugins/_ml/agents/";
    private static final String CONNECTORS_PATH = "/_plugins/_ml/connectors/";
    private static final String MODELS_PATH = "/_plugins/_ml/models/";
    private static final String MODEL_GROUPS_PATH = "/_plugins/_ml/model_groups/";

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
        assertOkOrAccepted(response);
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

        // Verify created resources and ml client calls for connector, model & agent
        response = makeRequest(tenantRequest, GET, WORKFLOW_PATH + workflowId + STATUS_ALL);
        assertOK(response);
        Map<String, Object> statemap = responseToMap(response);
        List<Map<String, Object>> resourcesCreated = (List<Map<String, Object>>) statemap.get("resources_created");
        String connectorId = resourcesCreated.get(0).get("resource_id").toString();
        assertNotNull(connectorId);

        String modelId = resourcesCreated.get(1).get("resource_id").toString();
        assertNotNull(modelId);

        String modelGroupId = resourcesCreated.get(2).get("resource_id").toString();
        assertNotNull(modelGroupId);

        String agentId = resourcesCreated.get(3).get("resource_id").toString();
        assertNotNull(agentId);

        // verify ml client call for Connector with valid tenant
        assertBusy(() -> {
            Response restResponse = makeRequest(tenantRequest, GET, CONNECTORS_PATH + connectorId);
            assertOK(restResponse);
            Map<String, Object> mlCommonsResponseMap = responseToMap(restResponse);
            if (multiTenancyEnabled) {
                assertEquals(tenantId, mlCommonsResponseMap.get(TENANT_ID_FIELD));
            } else {
                assertNull(mlCommonsResponseMap.get(TENANT_ID_FIELD));
            }
        }, 20, TimeUnit.SECONDS);

        // Now try again with an other ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(otherTenantRequest, GET, CONNECTORS_PATH + connectorId)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals("Failed to find connector with the provided connector id: " + connectorId, getErrorReasonFromResponseMap(map));
            } else {
                assertForbidden(response);
                assertEquals(NO_RESOURCE_ACCESS_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }
        } else {
            response = makeRequest(otherTenantRequest, GET, CONNECTORS_PATH + connectorId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("OpenAI Chat Connector", map.get("name"));
        }

        // Now try again with a null ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(
                ResponseException.class,
                () -> makeRequest(nullTenantRequest, GET, CONNECTORS_PATH + connectorId)
            );
            response = ex.getResponse();
            map = responseToMap(response);
            assertForbidden(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        } else {
            response = makeRequest(nullTenantRequest, GET, CONNECTORS_PATH + connectorId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("OpenAI Chat Connector", map.get("name"));
        }

        // verify ml client call for Model with valid tenant
        assertBusy(() -> {
            Response restResponse = makeRequest(tenantRequest, GET, MODELS_PATH + modelId);
            assertOK(restResponse);
            Map<String, Object> mlModelsResponseMap = responseToMap(restResponse);
            assertEquals("test model", mlModelsResponseMap.get("description"));
            if (multiTenancyEnabled) {
                assertEquals(tenantId, mlModelsResponseMap.get(TENANT_ID_FIELD));
            } else {
                assertNull(mlModelsResponseMap.get(TENANT_ID_FIELD));
            }
        }, 20, TimeUnit.SECONDS);

        // Now try again with an other ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(ResponseException.class, () -> makeRequest(otherTenantRequest, GET, MODELS_PATH + modelId));
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals("Failed to find model with the provided model id: " + modelId, getErrorReasonFromResponseMap(map));
            } else {
                assertForbidden(response);
                assertEquals(NO_RESOURCE_ACCESS_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }
        } else {
            response = makeRequest(otherTenantRequest, GET, MODELS_PATH + modelId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("test model", map.get("description"));
        }

        // Now try again with a null ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(ResponseException.class, () -> makeRequest(nullTenantRequest, GET, MODELS_PATH + modelId));
            response = ex.getResponse();
            assertForbidden(response);
            map = responseToMap(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        } else {
            response = makeRequest(nullTenantRequest, GET, MODELS_PATH + modelId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("test model", map.get("description"));
        }

        // verify ml client call for Agent with valid tenant
        assertBusy(() -> {
            Response restResponse = makeRequest(tenantRequest, GET, AGENTS_PATH + agentId);
            assertOK(restResponse);
            Map<String, Object> mlModelsResponseMap = responseToMap(restResponse);
            assertEquals("Test Agent", mlModelsResponseMap.get("name"));
            if (multiTenancyEnabled) {
                assertEquals(tenantId, mlModelsResponseMap.get(TENANT_ID_FIELD));
            } else {
                assertNull(mlModelsResponseMap.get(TENANT_ID_FIELD));
            }
        }, 20, TimeUnit.SECONDS);

        // Now try again with an other ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(ResponseException.class, () -> makeRequest(otherTenantRequest, GET, AGENTS_PATH + agentId));
            response = ex.getResponse();
            map = responseToMap(response);
            if (DDB) {
                assertNotFound(response);
                assertEquals("Failed to find agent with the provided agent id: " + agentId, getErrorReasonFromResponseMap(map));
            } else {
                assertForbidden(response);
                assertEquals(NO_RESOURCE_ACCESS_PERMISSION_REASON, getErrorReasonFromResponseMap(map));
            }
        } else {
            response = makeRequest(otherTenantRequest, GET, AGENTS_PATH + agentId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("Test Agent", map.get("name"));
        }

        // Now try again with a null ID
        if (multiTenancyEnabled) {
            ResponseException ex = assertThrows(ResponseException.class, () -> makeRequest(nullTenantRequest, GET, AGENTS_PATH + agentId));
            response = ex.getResponse();
            assertForbidden(response);
            map = responseToMap(response);
            assertEquals(MISSING_TENANT_REASON, getErrorReasonFromResponseMap(map));
        } else {
            response = makeRequest(nullTenantRequest, GET, AGENTS_PATH + agentId);
            assertOK(response);
            map = responseToMap(response);
            assertEquals("Test Agent", map.get("name"));
        }

        /*
         * Search
         */
        // Create and provision second workflow using otherTenantId
        RestRequest otherWorkflowRequest = getRestRequestWithHeadersAndContent(otherTenantId, createRemoteModelTemplate());
        response = makeRequest(otherWorkflowRequest, POST, WORKFLOW_URI + "?provision=true");
        assertOkOrAccepted(response);
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
                assertEquals(1, searchResponse.getHits().getTotalHits().value());
                assertEquals(tenantId, searchResponse.getHits().getHits()[0].getSourceAsMap().get(TENANT_ID_FIELD));
            } else {
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
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
                assertEquals(1, searchResponse.getHits().getTotalHits().value());
                assertEquals(otherTenantId, searchResponse.getHits().getHits()[0].getSourceAsMap().get(TENANT_ID_FIELD));
            } else {
                assertEquals(2, searchResponse.getHits().getTotalHits().value());
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
            assertEquals(2, searchResponse.getHits().getTotalHits().value());
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
        resourcesCreated = (List<Map<String, Object>>) map.get("resources_created");

        String otherConnectorId = resourcesCreated.get(0).get("resource_id").toString();
        assertNotNull(otherConnectorId);

        String otherModelId = resourcesCreated.get(1).get("resource_id").toString();
        assertNotNull(otherModelId);

        String otherModelGroupId = resourcesCreated.get(2).get("resource_id").toString();
        assertNotNull(otherModelGroupId);

        String otherAgentId = resourcesCreated.get(3).get("resource_id").toString();
        assertNotNull(otherAgentId);

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

        // verify if resources are deleted after de-provisioning
        ex = assertThrows(ResponseException.class, () -> makeRequest(otherTenantRequest, GET, CONNECTORS_PATH + otherConnectorId));
        response = ex.getResponse();
        map = responseToMap(response);
        assertNotFound(response);
        assertEquals("Failed to find connector with the provided connector id: " + otherConnectorId, getErrorReasonFromResponseMap(map));

        ex = assertThrows(ResponseException.class, () -> makeRequest(otherTenantRequest, GET, MODELS_PATH + otherModelId));
        response = ex.getResponse();
        assertNotFound(response);
        map = responseToMap(response);
        assertEquals("Failed to find model with the provided model id: " + otherModelId, getErrorReasonFromResponseMap(map));

        // Verify the deletion
        ex = assertThrows(ResponseException.class, () -> makeRequest(otherTenantRequest, GET, AGENTS_PATH + otherAgentId));
        response = ex.getResponse();
        assertNotFound(response);
        map = responseToMap(response);
        assertEquals("Failed to find agent with the provided agent id: " + otherAgentId, getErrorReasonFromResponseMap(map));

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
