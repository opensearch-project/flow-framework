/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.bwc;

import org.apache.http.HttpHeaders;
import org.apache.http.ParseException;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.TestHelpers;
import org.opensearch.flowframework.model.Template;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_ID;

public class FlowFrameworkBackwardsCompatibilityIT extends OpenSearchRestTestCase {

    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        setupSettings();
    }

    private void setupSettings() throws IOException {
        // Enable Flow Framework Plugin Rest APIs
        Response response = TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            "{\"transient\":{\"plugins.flow_framework.enabled\":true}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    private enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testBackwardsCompatibility() throws Exception {
        // This iteration of nodes is only to get plugins installed on each node. We don't currently use its functionality but in case we
        // ever need version-based dependencies in future BWC tests it will be needed. It's directly copied from similar implementations
        // in other plugins.
        String uri = getUri();
        Map<String, Map<String, Object>> responseMap = (Map<String, Map<String, Object>>) getAsMap(uri).get("nodes");
        for (Map<String, Object> response : responseMap.values()) {
            List<Map<String, Object>> plugins = (List<Map<String, Object>>) response.get("plugins");
            Set<Object> pluginNames = plugins.stream().map(map -> map.get("name")).collect(Collectors.toSet());
            assertTrue(pluginNames.contains("opensearch-flow-framework"));
            String workflowId = createNoopTemplate();
            Template t = getTemplate(workflowId);
            switch (CLUSTER_TYPE) {
                case OLD:
                    // mapping for 2.12 does not include time stamps
                    assertNull(t.createdTime());
                    assertNull(t.lastUpdatedTime());
                    assertNull(t.lastProvisionedTime());
                    break;
                case MIXED:
                    // Time stamps may or may not be null depending on whether index has been accessed by new version node
                    assertNull(t.lastProvisionedTime());
                    break;
                case UPGRADED:
                    // mapping for 2.13+ includes time stamps
                    assertNotNull(t.createdTime());
                    assertEquals(t.createdTime(), t.lastUpdatedTime());
                    assertNull(t.lastProvisionedTime());
                    break;
            }
            break;
        }
    }

    private String getUri() {
        switch (CLUSTER_TYPE) {
            case OLD:
                return "_nodes/" + CLUSTER_NAME + "-0/plugins";
            case MIXED:
                String round = System.getProperty("tests.rest.bwcsuite_round");
                if (round.equals("second")) {
                    return "_nodes/" + CLUSTER_NAME + "-1/plugins";
                } else if (round.equals("third")) {
                    return "_nodes/" + CLUSTER_NAME + "-2/plugins";
                } else {
                    return "_nodes/" + CLUSTER_NAME + "-0/plugins";
                }
            case UPGRADED:
                return "_nodes/plugins";
            default:
                throw new AssertionError("unknown cluster type: " + CLUSTER_TYPE);
        }
    }

    private String createNoopTemplate() throws IOException, ParseException {
        Response response = TestHelpers.makeRequest(
            client(),
            "POST",
            "_plugins/_flow_framework/workflow",
            null,
            "{\"name\":\"test\", \"workflows\":{\"provision\": {\"nodes\": [{\"id\":\"test-step\", \"type\":\"noop\"}]}}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(RestStatus.CREATED.getStatus(), response.getStatusLine().getStatusCode());

        Map<String, Object> responseMap = entityAsMap(response);
        String workflowId = (String) responseMap.get(WORKFLOW_ID);
        assertNotNull(workflowId);
        return workflowId;
    }

    private Template getTemplate(String workflowId) throws IOException, ParseException {
        Response response = TestHelpers.makeRequest(
            client(),
            "GET",
            "_plugins/_flow_framework/workflow/" + workflowId,
            null,
            "",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());

        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        return Template.parse(body);
    }
}
