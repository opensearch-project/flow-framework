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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.FlowFrameworkRestTestCase;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_RESOURCE_TYPE;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_RESOURCE_TYPE;

public class FlowFrameworkResourceSharingRestApiIT extends FlowFrameworkRestTestCase {

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
    String lionUser = "lion";
    RestClient lionClient;
    private String indexAllAccessRole = "index_all_access";
    private static String FLOW_FRAMEWORK_FULL_ACCESS_ROLE = "flow_framework_full_access";
    private static String ML_COMMONS_FULL_ACCESS_ROLE = "ml_full_access";
    private static String FLOW_FRAMEWORK_READ_ACCESS_ROLE = "flow_framework_read_access";

    // Sharing action-group names for Flow Framework (mirror AD style)
    private static final String WORKFLOW_READ_ONLY_AG = "workflow_read_only";
    private static final String WORKFLOW_FULL_ACCESS_AG = "workflow_full_access";
    private static final String WORKFLOW_STATE_READ_ONLY_AG = "workflow_state_read_only";
    private static final String WORKFLOW_STATE_FULL_ACCESS_AG = "workflow_state_full_access";

    // If the suite is launched without the flag, just skip these tests cleanly.
    private boolean skipTests = !isResourceSharingFeatureEnabled();

    @Before
    public void setupSecureTests() throws IOException {
        if (skipTests) {
            logger.info("Skipping FlowFrameworkResourceSharingRestApiIT tests - resource sharing not enabled");
            return;
        }

        if (!isHttps()) {
            fail("Secure Tests are running but HTTPS is not set");
        }

        createIndexRole(indexAllAccessRole, "*");
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

        String lionPassword = generatePassword(lionUser);
        createUser(lionUser, lionPassword, List.of("opensearch"));
        lionClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), lionUser, lionPassword)
            .setSocketTimeout(60000)
            .build();

        createRoleMapping(FLOW_FRAMEWORK_READ_ACCESS_ROLE, List.of(bobUser));
        createRoleMapping(ML_COMMONS_FULL_ACCESS_ROLE, List.of(aliceUser, catUser, dogUser, elkUser, fishUser));
        createRoleMapping(FLOW_FRAMEWORK_FULL_ACCESS_ROLE, List.of(aliceUser, catUser, dogUser, elkUser, fishUser));
        createRoleMapping(indexAllAccessRole, List.of(aliceUser));
    }

    @After
    public void tearDownSecureTests() throws IOException {
        if (skipTests) {
            return;
        }
        aliceClient.close();
        bobClient.close();
        catClient.close();
        dogClient.close();
        elkClient.close();
        fishClient.close();
        lionClient.close();
        deleteUser(aliceUser);
        deleteUser(bobUser);
        deleteUser(catUser);
        deleteUser(dogUser);
        deleteUser(elkUser);
        deleteUser(fishUser);
        deleteUser(lionUser);
    }

    public void testWorkflowVisibilityAndSearch_withResourceSharingEnabled() throws Exception {
        if (skipTests) {
            logger.info("Skipping test - resource sharing not enabled");
            return;
        }
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response created = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(created));
        String workflowId = (String) entityAsMap(created).get(WORKFLOW_ID);

        // Unshared → cat/admin cannot GET
        ResponseException ex = expectThrows(ResponseException.class, () -> getWorkflow(catClient, workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());
        ex = expectThrows(ResponseException.class, () -> getWorkflow(client(), workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());

        // Owner search sees ≥1; others see 0
        String matchAll = "{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}}}";
        SearchResponse ownerSr = searchWorkflows(aliceClient, matchAll);
        assertEquals(RestStatus.OK, ownerSr.status());
        assertTrue(ownerSr.getHits().getTotalHits().value() >= 1);

        SearchResponse catSr = searchWorkflows(catClient, matchAll);
        assertEquals(0, catSr.getHits().getTotalHits().value());
        SearchResponse adminSr = searchWorkflows(client(), matchAll);
        assertEquals(0, adminSr.getHits().getTotalHits().value());

        // Share READ_ONLY with cat → cat can GET & search=1
        Response shareRO = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_RESOURCE_TYPE, WORKFLOW_READ_ONLY_AG, catUser)
        );
        assertEquals(200, shareRO.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, catClient);

        Response catGet = getWorkflow(catClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(catGet));
        catSr = searchWorkflows(catClient, matchAll);
        assertEquals(1, catSr.getHits().getTotalHits().value());

        // Admin still unshared → 0
        adminSr = searchWorkflows(client(), matchAll);
        assertEquals(0, adminSr.getHits().getTotalHits().value());

        // Grant FULL_ACCESS to elk → elk can share to backend role "aes" as READ_ONLY
        Response grantFullToElk = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_RESOURCE_TYPE, WORKFLOW_FULL_ACCESS_AG, elkUser)
        );
        assertEquals(200, grantFullToElk.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, elkClient);

        var recs = new HashMap<Recipient, Set<String>>();
        recs.put(Recipient.BACKEND_ROLES, Set.of("aes"));
        PatchSharingInfoPayloadBuilder patch = new PatchSharingInfoPayloadBuilder().configId(workflowId).configType(WORKFLOW_RESOURCE_TYPE);
        patch.share(recs, WORKFLOW_READ_ONLY_AG);

        Response elkAddsAes = patchSharingInfo(elkClient, Map.of(), patch.build());
        assertEquals(200, elkAddsAes.getStatusLine().getStatusCode());

        // Revoke user-level cat; cat still sees via backend role "aes"
        recs = new HashMap<>();
        recs.put(Recipient.USERS, Set.of(catUser));
        patch = new PatchSharingInfoPayloadBuilder().configId(workflowId).configType(WORKFLOW_RESOURCE_TYPE);
        patch.revoke(recs, WORKFLOW_READ_ONLY_AG);

        Response elkRevokesUserCat = patchSharingInfo(elkClient, Map.of(), patch.build());
        assertEquals(200, elkRevokesUserCat.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, catClient); // still visible via backend role

        // Now revoke backend role "aes" → cat loses access
        recs = new HashMap<>();
        recs.put(Recipient.BACKEND_ROLES, Set.of("aes"));
        patch = new PatchSharingInfoPayloadBuilder().configId(workflowId).configType(WORKFLOW_RESOURCE_TYPE);
        patch.revoke(recs, WORKFLOW_READ_ONLY_AG);

        Response elkRevokesAes = patchSharingInfo(elkClient, Map.of(), patch.build());
        assertEquals(200, elkRevokesAes.getStatusLine().getStatusCode());
        waitForWorkflowRevokeNonVisibility(workflowId, catClient);
    }

    public void testWorkflowUpdate_withResourceSharingEnabled() throws Exception {
        if (skipTests) {
            return;
        }
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response created = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(created));
        String workflowId = (String) entityAsMap(created).get(WORKFLOW_ID);

        // Unshared → admin/fish/cat cannot update
        ResponseException ex = expectThrows(ResponseException.class, () -> updateWorkflow(client(), workflowId, template));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());
        ex = expectThrows(ResponseException.class, () -> updateWorkflow(fishClient, workflowId, template));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());

        // READ_ONLY to cat → still cannot update
        Response shareRO = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_RESOURCE_TYPE, WORKFLOW_READ_ONLY_AG, catUser)
        );
        assertEquals(200, shareRO.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, catClient);

        ex = expectThrows(ResponseException.class, () -> updateWorkflow(catClient, workflowId, template));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());

        // FULL_ACCESS to elk → elk can update
        Response shareFull = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_RESOURCE_TYPE, WORKFLOW_FULL_ACCESS_AG, elkUser)
        );
        assertEquals(200, shareFull.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, elkClient);

        Response elkUpdate = updateWorkflow(elkClient, workflowId, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(elkUpdate));

        // Revoke elk FULL_ACCESS → elk loses update ability
        var recs = new HashMap<Recipient, Set<String>>();
        recs.put(Recipient.USERS, Set.of(elkUser));
        PatchSharingInfoPayloadBuilder patch = new PatchSharingInfoPayloadBuilder().configId(workflowId).configType(WORKFLOW_RESOURCE_TYPE);
        patch.revoke(recs, WORKFLOW_FULL_ACCESS_AG);

        Response revoked = patchSharingInfo(aliceClient, Map.of(), patch.build());
        assertEquals(200, revoked.getStatusLine().getStatusCode());
        waitForWorkflowRevokeNonVisibility(workflowId, elkClient);

        ResponseException elkDenied = expectThrows(ResponseException.class, () -> updateWorkflow(elkClient, workflowId, template));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), elkDenied.getResponse().getStatusLine().getStatusCode());
    }

    public void testProvisionDeprovision_withResourceSharingEnabled() throws Exception {
        if (skipTests) {
            return;
        }
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response created = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(created));
        String workflowId = (String) entityAsMap(created).get(WORKFLOW_ID);

        // Unshared → cat cannot provision/deprovision
        ResponseException ex = expectThrows(ResponseException.class, () -> provisionWorkflow(catClient, workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());
        ex = expectThrows(ResponseException.class, () -> deprovisionWorkflow(catClient, workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());

        // READ_ONLY to cat → still cannot provision/deprovision
        Response shareRO = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_RESOURCE_TYPE, WORKFLOW_READ_ONLY_AG, catUser)
        );
        assertEquals(200, shareRO.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, catClient);

        ex = expectThrows(ResponseException.class, () -> provisionWorkflow(catClient, workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());

        // FULL_ACCESS to elk → elk can provision & deprovision
        Response shareFull = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_RESOURCE_TYPE, WORKFLOW_FULL_ACCESS_AG, elkUser)
        );
        assertEquals(200, shareFull.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, elkClient);

        Response prov = provisionWorkflow(elkClient, workflowId);
        assertEquals(RestStatus.ACCEPTED, TestHelpers.restStatus(prov));

        Response deprov = deprovisionWorkflow(elkClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(deprov));

        // Revoke elk FULL_ACCESS → elk loses ability
        var recs = new HashMap<Recipient, Set<String>>();
        recs.put(Recipient.USERS, Set.of(elkUser));
        PatchSharingInfoPayloadBuilder patch = new PatchSharingInfoPayloadBuilder().configId(workflowId).configType(WORKFLOW_RESOURCE_TYPE);
        patch.revoke(recs, WORKFLOW_FULL_ACCESS_AG);

        Response revoked = patchSharingInfo(aliceClient, Map.of(), patch.build());
        assertEquals(200, revoked.getStatusLine().getStatusCode());
        waitForWorkflowRevokeNonVisibility(workflowId, elkClient);

        ResponseException elkDenied = expectThrows(ResponseException.class, () -> provisionWorkflow(elkClient, workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), elkDenied.getResponse().getStatusLine().getStatusCode());
    }

    public void testDeleteWorkflow_withResourceSharingEnabled() throws Exception {
        if (skipTests) {
            return;
        }
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response created = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(created));
        String workflowId = (String) entityAsMap(created).get(WORKFLOW_ID);

        // Unshared → cat/admin cannot delete
        ResponseException ex = expectThrows(ResponseException.class, () -> deleteWorkflow(catClient, workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());
        ex = expectThrows(ResponseException.class, () -> deleteWorkflow(client(), workflowId));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());

        // FULL_ACCESS → can delete
        Response grantFull = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_RESOURCE_TYPE, WORKFLOW_FULL_ACCESS_AG, catUser)
        );
        assertEquals(200, grantFull.getStatusLine().getStatusCode());
        waitForWorkflowSharingVisibility(workflowId, catClient);

        Response del = deleteWorkflow(catClient, workflowId);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(del));
    }

    public void testWorkflowStateVisibilityAndSearch_withResourceSharingEnabled() throws Exception {
        if (skipTests) {
            return;
        }
        // Create a workflow (state doc gets created/managed by FF)
        Template template = TestHelpers.createTemplateFromFile("register-deploylocalsparseencodingmodel.json");
        Response created = createWorkflow(aliceClient, template);
        assertEquals(RestStatus.CREATED, TestHelpers.restStatus(created));
        String workflowId = (String) entityAsMap(created).get(WORKFLOW_ID);

        // Unshared state → cat/admin cannot read status or find it
        ResponseException ex = expectThrows(ResponseException.class, () -> getWorkflowStatus(catClient, workflowId, false));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());
        ex = expectThrows(ResponseException.class, () -> getWorkflowStatus(client(), workflowId, false));
        assertEquals(RestStatus.FORBIDDEN.getStatus(), ex.getResponse().getStatusLine().getStatusCode());

        String matchAllState = "{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}]}}}";
        SearchResponse ownerSr = searchWorkflowState(aliceClient, matchAllState);
        assertEquals(RestStatus.OK, ownerSr.status());
        // others see 0
        SearchResponse catSr = searchWorkflowState(catClient, matchAllState);
        assertEquals(0, catSr.getHits().getTotalHits().value());
        SearchResponse adminSr = searchWorkflowState(client(), matchAllState);
        assertEquals(0, adminSr.getHits().getTotalHits().value());

        // Share workflow_state READ_ONLY to cat → cat can read status & search sees 1
        Response shareStateRO = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_STATE_RESOURCE_TYPE, WORKFLOW_STATE_READ_ONLY_AG, catUser)
        );
        assertEquals(200, shareStateRO.getStatusLine().getStatusCode());
        // status endpoint visibility check (reuse the same wait helper by hitting GET workflow status in the lambda)
        waitForWorkflowStateSharingVisibility(workflowId, catClient);

        Response catStatus = getWorkflowStatus(catClient, workflowId, false);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(catStatus));
        catSr = searchWorkflowState(catClient, matchAllState);
        assertTrue(catSr.getHits().getTotalHits().value() >= 1);

        // Grant workflow_state FULL_ACCESS to elk → elk can also read/modify state if any write-state APIs exist
        Response shareStateFull = shareConfig(
            aliceClient,
            Map.of(),
            shareWithUserPayload(workflowId, WORKFLOW_STATE_RESOURCE_TYPE, WORKFLOW_STATE_FULL_ACCESS_AG, elkUser)
        );
        assertEquals(200, shareStateFull.getStatusLine().getStatusCode());
        // sanity check visibility
        Response elkStatus = getWorkflowStatus(elkClient, workflowId, false);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(elkStatus));

        // Revoke cat (READ_ONLY on state) → cat loses status visibility
        var recs = new HashMap<Recipient, Set<String>>();
        recs.put(Recipient.USERS, Set.of(catUser));
        PatchSharingInfoPayloadBuilder patch = new PatchSharingInfoPayloadBuilder().configId(workflowId)
            .configType(WORKFLOW_STATE_RESOURCE_TYPE);
        patch.revoke(recs, WORKFLOW_STATE_READ_ONLY_AG);

        Response revoked = patchSharingInfo(aliceClient, Map.of(), patch.build());
        assertEquals(200, revoked.getStatusLine().getStatusCode());

        // wait until cat can no longer read status
        waitForWorkflowStateRevokeNonVisibility(workflowId, catClient);
    }
}
