/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import com.google.gson.JsonArray;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.repackage.com.google.common.collect.ImmutableList;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

/**
 * Base rest integration test class, supports security enabled/disabled cluster
 */
public abstract class FlowFrameworkRestTestCase extends OpenSearchRestTestCase {

    public static final String SHARE_WORKFLOW_URI = "/_plugins/_security/api/resource/share";

    @Before
    protected void setUpSettings() throws Exception {

        // Enable ML Commons to run on non-ml nodes
        Response response = TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            "{\"persistent\":{\"plugins.ml_commons.only_run_on_ml_node\":false}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Enable local model registration via URL
        response = TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            "{\"persistent\":{\"plugins.ml_commons.allow_registering_model_via_url\":true}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Set ML jvm heap memory threshold to 100 to avoid opening the circuit breaker during tests
        response = TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            "{\"persistent\":{\"plugins.ml_commons.jvm_heap_memory_threshold\":100}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Set ML auto redeploy to false
        response = TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            "{\"persistent\":{\"plugins.ml_commons.model_auto_redeploy.enable\":false}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(200, response.getStatusLine().getStatusCode());

        // Set ML auto redeploy retries to 0
        response = TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            "{\"persistent\":{\"plugins.ml_commons.model_auto_redeploy.lifetime_retry_times\":0}}",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        assertEquals(200, response.getStatusLine().getStatusCode());

    }

    protected boolean isHttps() {
        return Optional.ofNullable(System.getProperty("https")).map("true"::equalsIgnoreCase).orElse(false);
    }

    @Override
    protected Settings restClientSettings() {
        return super.restClientSettings();
    }

    @Override
    protected String getProtocol() {
        return isHttps() ? "https" : "http";
    }

    public static Map<String, Object> responseToMap(Response response) throws IOException {
        HttpEntity entity = response.getEntity();
        assertNotNull(response);
        String entityString = TestHelpers.httpEntityToString(entity);
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            entityString
        );
        parser.nextToken();
        return parser.map();
    }

    // Utility fn for deleting indices. Should only be used when not allowed in a regular context
    // (e.g., deleting system indices)
    protected static void deleteIndexWithAdminClient(String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        adminClient().performRequest(request);
    }

    // Utility fn for checking if an index exists. Should only be used when not allowed in a regular context
    // (e.g., checking existence of system indices)
    public static boolean indexExistsWithAdminClient(String indexName) throws IOException {
        Request request = new Request("HEAD", "/" + indexName);
        Response response = adminClient().performRequest(request);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isHttps()) {
            configureHttpsClient(builder, settings);
        } else {
            configureClient(builder, settings);
        }

        builder.setStrictDeprecationMode(false);
        return builder.build();
    }

    protected static void configureHttpsClient(RestClientBuilder builder, Settings settings) throws IOException {
        // Similar to client configuration with OpenSearch:
        // https://github.com/opensearch-project/OpenSearch/blob/2.11.1/test/framework/src/main/java/org/opensearch/test/rest/OpenSearchRestTestCase.java#L841-L863
        // except we set the user name and password
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            String userName = Optional.ofNullable(System.getProperty("user"))
                .orElseThrow(() -> new RuntimeException("user name is missing"));
            String password = Optional.ofNullable(System.getProperty("password"))
                .orElseThrow(() -> new RuntimeException("password is missing"));
            BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            final AuthScope anyScope = new AuthScope(null, -1);
            credentialsProvider.setCredentials(anyScope, new UsernamePasswordCredentials(userName, password.toCharArray()));
            try {
                final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSslContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build())
                    // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                    .setTlsDetailsFactory(sslEngine -> new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol()))
                    .build();
                final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                    .setTlsStrategy(tlsStrategy)
                    .build();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(
            conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis())))
        );
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    @SuppressWarnings("unchecked")
    @After
    protected void wipeAllODFEIndices() throws IOException {
        Response response = adminClient().performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
        MediaType xContentType = MediaType.fromMediaType(response.getEntity().getContentType());
        try (
            XContentParser parser = xContentType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            XContentParser.Token token = parser.nextToken();
            List<Map<String, Object>> parserList = null;
            if (token == XContentParser.Token.START_ARRAY) {
                parserList = parser.listOrderedMap().stream().map(obj -> (Map<String, Object>) obj).collect(Collectors.toList());
            } else {
                parserList = Collections.singletonList(parser.mapOrdered());
            }

            for (Map<String, Object> index : parserList) {
                String indexName = (String) index.get("index");
                if (indexName != null) {
                    // Filter out system index deletion only when running security enabled tests
                    if (isHttps() && isSystemIndex(indexName)) {
                        continue;
                    }
                    // Do not reset ML/Flow Framework encryption index as this is needed to encrypt connector credentials
                    if (!".plugins-ml-config".equals(indexName) && !".plugins-flow-framework-config".equals(indexName)) {
                        adminClient().performRequest(new Request("DELETE", "/" + indexName));
                    }
                }
            }
        }
    }

    private boolean isSystemIndex(String indexName) {
        return ".opendistro_security".equals(indexName)
            || indexName.startsWith(".opensearch")
            || indexName.startsWith(".plugins-flow-framework")
            || indexName.startsWith(".plugins-ml");
    }

    /**
     * wipeAllIndices won't work since it cannot delete security index. Use wipeAllSystemIndices instead.
     */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    /**
     * Required to persist cluster settings between test executions
     */
    @Override
    protected boolean preserveClusterSettings() {
        return true;
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

    /**
     * Helper method to invoke the Create Workflow Rest Action without validation
     * @param client the rest client
     * @param template the template to create
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflow(RestClient client, Template template) throws Exception {
        return TestHelpers.makeRequest(client, "POST", WORKFLOW_URI + "?validation=off", Collections.emptyMap(), template.toJson(), null);
    }

    /**
     * Helper method to invoke the Create Workflow Rest Action without validation
     * @param client the rest client
     * @param useCase the usecase to create
     * @param params the required params
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflowWithUseCaseWithNoValidation(RestClient client, String useCase, List<String> params) throws Exception {

        StringBuilder sb = new StringBuilder();
        for (String param : params) {
            sb.append('"').append(param).append("\" : \"\",");
        }
        if (!params.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }

        return TestHelpers.makeRequest(
            client,
            "POST",
            WORKFLOW_URI + "?validation=off&use_case=" + useCase,
            Collections.emptyMap(),
            "{" + sb.toString() + "}",
            null
        );
    }

    public Response createIndexRole(String role, String index) throws IOException {
        return TestHelpers.makeRequest(
            client(),
            "PUT",
            "/_plugins/_security/api/roles/" + role,
            null,
            TestHelpers.toHttpEntity(
                "{\n"
                    + "\"cluster_permissions\": [\n"
                    + "\"cluster:admin/ingest/pipeline/put\",\n"
                    + "\"cluster:admin/ingest/pipeline/delete\"\n"
                    + "],\n"
                    + "\"index_permissions\": [\n"
                    + "{\n"
                    + "\"index_patterns\": [\n"
                    + "\""
                    + index
                    + "\"\n"
                    + "],\n"
                    + "\"dls\": \"\",\n"
                    + "\"fls\": [],\n"
                    + "\"masked_fields\": [],\n"
                    + "\"allowed_actions\": [\n"
                    + "\"crud\",\n"
                    + "\"indices:admin/create\",\n"
                    + "\"indices:admin/aliases\",\n"
                    + "\"indices:admin/settings/update\",\n"
                    + "\"indices:admin/delete\"\n"
                    + "]\n"
                    + "}\n"
                    + "],\n"
                    + "\"tenant_permissions\": []\n"
                    + "}"
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
    }

    public Response createRoleMapping(String role, List<String> users) throws IOException {
        JsonArray usersString = new JsonArray();
        for (int i = 0; i < users.size(); i++) {
            usersString.add(users.get(i));
        }
        return TestHelpers.makeRequest(
            client(),
            "PUT",
            "/_plugins/_security/api/rolesmapping/" + role,
            null,
            TestHelpers.toHttpEntity(
                "{\n" + "  \"backend_roles\" : [  ],\n" + "  \"hosts\" : [  ],\n" + "  \"users\" : " + usersString + "\n" + "}"
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
    }

    public Response enableFilterBy() throws IOException {
        return TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            TestHelpers.toHttpEntity(
                "{\n" + "  \"persistent\": {\n" + "       \"plugins.flow_framework.filter_by_backend_roles\" : \"true\"\n" + "   }\n" + "}"
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
    }

    public Response disableFilterBy() throws IOException {
        return TestHelpers.makeRequest(
            client(),
            "PUT",
            "_cluster/settings",
            null,
            TestHelpers.toHttpEntity(
                "{\n" + "  \"persistent\": {\n" + "       \"plugins.flow_framework.filter_by_backend_roles\" : \"false\"\n" + "   }\n" + "}"
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
    }

    public void confirmingClientIsAdmin() throws IOException {
        Response resp = TestHelpers.makeRequest(
            client(),
            "GET",
            "_plugins/_security/api/account",
            null,
            "",
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "admin"))
        );
        Map<String, Object> responseMap = entityAsMap(resp);
        ArrayList<String> roles = (ArrayList<String>) responseMap.get("roles");
        assertTrue(roles.contains("all_access"));
    }

    /**
     * Helper method to invoke the create workflow API with a use case and also the provision param as true
     * @param client the rest client
     * @param useCase the usecase to create
     * @param defaults the defaults to override given through the request payload
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createAndProvisionWorkflowWithUseCaseWithContent(RestClient client, String useCase, Map<String, Object> defaults)
        throws Exception {
        String payload = ParseUtils.parseArbitraryStringToObjectMapToString(defaults);

        return TestHelpers.makeRequest(
            client,
            "POST",
            WORKFLOW_URI + "?provision=true&use_case=" + useCase,
            Collections.emptyMap(),
            payload,
            null
        );
    }

    /**
     * Helper method to invoke the Create Workflow Rest Action with provision
     * @param client the rest client
     * @param template the template to create
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflowWithProvision(RestClient client, Template template) throws Exception {
        return TestHelpers.makeRequest(client, "POST", WORKFLOW_URI + "?provision=true", Collections.emptyMap(), template.toJson(), null);
    }

    /**
     * Helper method to invoke the Create Workflow Rest Action with validation
     * @param client the rest client
     * @param template the template to create
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflowValidation(RestClient client, Template template) throws Exception {
        return TestHelpers.makeRequest(client, "POST", WORKFLOW_URI, Collections.emptyMap(), template.toJson(), null);
    }

    /**
     * Helper method to invoke the Reprovision Workflow API
     * @param client the rest client
     * @param workflowId the document id
     * @param template the template to reprovision
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response reprovisionWorkflow(RestClient client, String workflowId, Template template) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "PUT",
            String.format(Locale.ROOT, "%s/%s?reprovision=true", WORKFLOW_URI, workflowId),
            Collections.emptyMap(),
            template.toJson(),
            null
        );
    }

    /**
     * Helper method to invoke the Update Workflow API
     * @param client the rest client
     * @param workflowId the document id
     * @param template the template used to update
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response updateWorkflow(RestClient client, String workflowId, Template template) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "PUT",
            String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, workflowId),
            Collections.emptyMap(),
            template.toJson(),
            null
        );
    }

    /**
     * Helper method to invoke the Update Workflow API
     * @param client the rest client
     * @param workflowId the document id
     * @param templateFields the JSON containing some template fields
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response updateWorkflowWithFields(RestClient client, String workflowId, String templateFields) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "PUT",
            String.format(Locale.ROOT, "%s/%s?update_fields=true", WORKFLOW_URI, workflowId),
            Collections.emptyMap(),
            templateFields,
            null
        );
    }

    /**
     * Helper method to invoke the Provision Workflow Rest Action
     * @param client the rest client
     * @param workflowId the workflow ID to provision
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response provisionWorkflow(RestClient client, String workflowId) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "POST",
            String.format(Locale.ROOT, "%s/%s/%s", WORKFLOW_URI, workflowId, "_provision"),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Deprovision Workflow Rest Action
     * @param client the rest client
     * @param workflowId the workflow ID to deprovision
     * @return a rest response
     * @throws Exception if the request fails
     */
    protected Response deprovisionWorkflow(RestClient client, String workflowId) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "POST",
            String.format(Locale.ROOT, "%s/%s/%s", WORKFLOW_URI, workflowId, "_deprovision"),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Deprovision Workflow Rest Action
     * @param client the rest client
     * @param workflowId the workflow ID to deprovision
     * @return a rest response
     * @throws Exception if the request fails
     */
    protected Response deprovisionWorkflowWithAllowDelete(RestClient client, String workflowId, String allowedResource) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "POST",
            String.format(Locale.ROOT, "%s/%s/%s%s", WORKFLOW_URI, workflowId, "_deprovision?allow_delete=", allowedResource),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Delete Workflow Rest Action
     * @param client the rest client
     * @param workflowId the workflow ID to delete
     * @return a rest response
     * @throws Exception if the request fails
     */
    protected Response deleteWorkflow(RestClient client, String workflowId) throws Exception {
        return deleteWorkflow(client, workflowId, "");
    }

    /**
     * Helper method to invoke the Delete Workflow Rest Action
     * @param client the rest client
     * @param workflowId the workflow ID to delete
     * @param params a string adding any rest path params
     * @return a rest response
     * @throws Exception if the request fails
     */
    protected Response deleteWorkflow(RestClient client, String workflowId, String params) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "DELETE",
            String.format(Locale.ROOT, "%s/%s%s", WORKFLOW_URI, workflowId, params),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Get Workflow Rest Action
     * @param client the rest client
     * @param workflowId the workflow ID to get the status
     * @param all verbose status flag
     * @throws Exception if the request fails
     * @return rest response
     */
    protected Response getWorkflowStatus(RestClient client, String workflowId, boolean all) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "GET",
            String.format(Locale.ROOT, "%s/%s/%s?all=%s", WORKFLOW_URI, workflowId, "_status", all),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Get Workflow Rest Action
     * @param client the rest client
     * @param workflowId the workflow ID
     * @return rest response
     * @throws Exception
     */
    protected Response getWorkflow(RestClient client, String workflowId) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "GET",
            String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, workflowId),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Get Workflow Steps Rest Action
     * @param client the rest client
     * @return rest response
     * @throws Exception
     */
    protected Response getWorkflowStep(RestClient client) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "GET",
            String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, "_steps"),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Get Agent Rest Action
     * @param client the rest client
     * @return rest response
     * @throws Exception
     */
    protected Response getAgent(RestClient client, String agentId) throws Exception {
        return TestHelpers.makeRequest(
            client,
            "GET",
            String.format(Locale.ROOT, "/_plugins/_ml/agents/%s", agentId),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Search Workflow Rest Action with the given query
     * @param client the rest client
     * @param query the search query
     * @return rest response
     * @throws Exception if the request fails
     */
    protected SearchResponse searchWorkflows(RestClient client, String query) throws Exception {
        // Execute search
        Response restSearchResponse = TestHelpers.makeRequest(
            client,
            "GET",
            String.format(Locale.ROOT, "%s/_search", WORKFLOW_URI),
            Collections.emptyMap(),
            query,
            null
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(restSearchResponse));

        // Parse entity content into SearchResponse
        MediaType mediaType = MediaType.fromMediaType(restSearchResponse.getEntity().getContentType());
        try (
            XContentParser parser = mediaType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    restSearchResponse.getEntity().getContent()
                )
        ) {
            return SearchResponse.fromXContent(parser);
        }
    }

    /**
     * Helper method to invoke the Search Workflow State Rest Action
     * @param client the rest client
     * @param query the search query
     * @return
     * @throws Exception
     */
    protected SearchResponse searchWorkflowState(RestClient client, String query) throws Exception {
        Response restSearchResponse = TestHelpers.makeRequest(
            client,
            "GET",
            String.format(Locale.ROOT, "%s/state/_search", WORKFLOW_URI),
            Collections.emptyMap(),
            query,
            null
        );
        assertEquals(RestStatus.OK, TestHelpers.restStatus(restSearchResponse));

        // Parse entity content into SearchResponse
        MediaType mediaType = MediaType.fromMediaType(restSearchResponse.getEntity().getContentType());
        try (
            XContentParser parser = mediaType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    restSearchResponse.getEntity().getContent()
                )
        ) {
            return SearchResponse.fromXContent(parser);
        }
    }

    /**
     * Helper method to invoke the Get Workflow Status Rest Action and assert the provisioning and state status
     * @param client the rest client
     * @param workflowId the workflow ID to get the status
     * @param stateStatus the state status name
     * @param provisioningStatus the provisioning status name
     * @throws Exception if the request fails
     */
    protected void getAndAssertWorkflowStatus(
        RestClient client,
        String workflowId,
        State stateStatus,
        ProvisioningProgress provisioningStatus
    ) throws Exception {
        Response response = getWorkflowStatus(client, workflowId, true);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        assertEquals(stateStatus.name(), (String) responseMap.get(CommonValue.STATE_FIELD));
        assertEquals(provisioningStatus.name(), (String) responseMap.get(CommonValue.PROVISIONING_PROGRESS_FIELD));
    }

    /**
     * Helper method to invoke the Get Workflow Status Rest Action and assert document is not found
     * @param client the rest client
     * @param workflowId the workflow ID to get the status
     * @throws Exception if the request fails
     */
    protected void getAndAssertWorkflowStatusNotFound(RestClient client, String workflowId) throws Exception {
        ResponseException ex = assertThrows(ResponseException.class, () -> getWorkflowStatus(client, workflowId, true));
        assertEquals(RestStatus.NOT_FOUND.getStatus(), ex.getResponse().getStatusLine().getStatusCode());
    }

    /**
     * Helper method to invoke the Get Workflow status Rest Action and get the error field
     * @param client the rest client
     * @param workflowId the workflow ID to get the status
     * @return the error string
     * @throws Exception if the request fails
     */
    protected String getAndWorkflowStatusError(RestClient client, String workflowId) throws Exception {
        Response response = getWorkflowStatus(client, workflowId, true);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        return (String) responseMap.get(CommonValue.ERROR_FIELD);
    }

    /**
     * Helper method to get and assert a workflow step
     * @param client the rest client
     * @throws Exception
     */
    protected void getAndAssertWorkflowStep(RestClient client) throws Exception {
        Response response = getWorkflowStep(client);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    /**
     * Helper method to wait until a workflow provisioning has completed and retrieve any resources created
     * @param client the rest client
     * @param workflowId the workflow id to retrieve resources from
     * @param timeout the max wait time in seconds
     * @return a list of created resources
     * @throws Exception if the request fails
     */
    protected List<ResourceCreated> getResourcesCreated(RestClient client, String workflowId, int timeout) throws Exception {

        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(client, workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            timeout,
            TimeUnit.SECONDS
        );

        return getResourcesCreated(client, workflowId);
    }

    /**
     * Helper method retrieve any resources created incrementally without waiting for completion
     * @param client the rest client
     * @param workflowId the workflow id to retrieve resources from
     * @return a list of created resources
     * @throws Exception if the request fails
     */
    protected List<ResourceCreated> getResourcesCreated(RestClient client, String workflowId) throws Exception {
        Response response = getWorkflowStatus(client, workflowId, true);

        // Parse workflow state from response and retrieve resources created
        MediaType mediaType = MediaType.fromMediaType(response.getEntity().getContentType());
        try (
            XContentParser parser = mediaType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
        ) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            WorkflowState workflowState = WorkflowState.parse(parser);
            return workflowState.resourcesCreated();
        }
    }

    public Response createUser(String name, String password, List<String> backendRoles) throws IOException {
        JsonArray backendRolesString = new JsonArray();
        for (int i = 0; i < backendRoles.size(); i++) {
            backendRolesString.add(backendRoles.get(i));
        }
        return TestHelpers.makeRequest(
            client(),
            "PUT",
            "/_plugins/_security/api/internalusers/" + name,
            null,
            TestHelpers.toHttpEntity(
                " {\n"
                    + "\"password\": \""
                    + password
                    + "\",\n"
                    + "\"backend_roles\": "
                    + backendRolesString
                    + ",\n"
                    + "\"attributes\": {\n"
                    + "}} "
            ),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
    }

    protected Response deleteUser(String user) throws IOException {
        return TestHelpers.makeRequest(
            client(),
            "DELETE",
            "/_plugins/_security/api/internalusers/" + user,
            null,
            "",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
    }

    protected GetPipelineResponse getPipelines(String pipelineId) throws IOException {
        Response getPipelinesResponse = TestHelpers.makeRequest(
            client(),
            "GET",
            "_ingest/pipeline/" + pipelineId,
            null,
            "",
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );

        // Parse entity content into SearchResponse
        MediaType mediaType = MediaType.fromMediaType(getPipelinesResponse.getEntity().getContentType());
        try (
            XContentParser parser = mediaType.xContent()
                .createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    getPipelinesResponse.getEntity().getContent()
                )
        ) {
            return GetPipelineResponse.fromXContent(parser);
        }
    }

    protected void ingestSingleDoc(String payload, String indexName) throws IOException {
        try {
            TestHelpers.makeRequest(
                client(),
                "PUT",
                indexName + "/_doc/1",
                null,
                payload,
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected SearchResponse neuralSearchRequest(String indexName, String modelId) throws IOException {
        String searchRequest =
            "{\"_source\":{\"excludes\":[\"passage_embedding\"]},\"query\":{\"neural\":{\"passage_embedding\":{\"query_text\":\"world\",\"k\":5,\"model_id\":\""
                + modelId
                + "\"}}}}";
        try {
            Response restSearchResponse = TestHelpers.makeRequest(
                client(),
                "POST",
                indexName + "/_search",
                null,
                searchRequest,
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
            );
            // Parse entity content into SearchResponse
            MediaType mediaType = MediaType.fromMediaType(restSearchResponse.getEntity().getContentType());
            try (
                XContentParser parser = mediaType.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        restSearchResponse.getEntity().getContent()
                    )
            ) {
                return SearchResponse.fromXContent(parser);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    protected List<String> catPlugins() throws IOException {
        Response response = TestHelpers.makeRequest(
            client(),
            "GET",
            "_cat/plugins?s=component&h=name,component,version,description&format=json",
            null,
            "",
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        List<Object> pluginsList = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).list();
        return pluginsList.stream().map(o -> ((Map<String, Object>) o).get("component").toString()).collect(Collectors.toList());
    }

    protected boolean isResourceSharingFeatureEnabled() {
        return Optional.ofNullable(System.getProperty("resource_sharing.enabled")).map("true"::equalsIgnoreCase).orElse(false);
    }

    public static Response shareConfig(RestClient client, Map<String, String> params, String payload) throws IOException {
        return TestHelpers.makeRequest(client, "PUT", SHARE_WORKFLOW_URI, params, payload, null);
    }

    public static Response patchSharingInfo(RestClient client, Map<String, String> params, String payload) throws IOException {
        return TestHelpers.makeRequest(client, "PATCH", SHARE_WORKFLOW_URI, params, payload, null);
    }

    public static String shareWithUserPayload(String resourceId, String resourceIndex, String accessLevel, String user) {
        return String.format(Locale.ROOT, """
            {
              "resource_id": "%s",
              "resource_type": "%s",
              "share_with": {
                "%s" : {
                    "users": ["%s"]
                }
              }
            }
            """, resourceId, resourceIndex, accessLevel, user);
    }

    public enum Recipient {
        USERS("users"),
        ROLES("roles"),
        BACKEND_ROLES("backend_roles");

        private final String name;

        Recipient(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static class PatchSharingInfoPayloadBuilder {
        private String configId;
        private String configType;

        // accessLevel -> recipientType -> principals
        private final Map<String, Map<String, Set<String>>> share = new HashMap<>();
        private final Map<String, Map<String, Set<String>>> revoke = new HashMap<>();

        public PatchSharingInfoPayloadBuilder configId(String resourceId) {
            this.configId = resourceId;
            return this;
        }

        public PatchSharingInfoPayloadBuilder configType(String resourceType) {
            this.configType = resourceType;
            return this;
        }

        public void share(Map<Recipient, Set<String>> recipients, String accessLevel) {
            mergeInto(share, accessLevel, recipients);
        }

        public void revoke(Map<Recipient, Set<String>> recipients, String accessLevel) {
            mergeInto(revoke, accessLevel, recipients);
        }

        /* -------------------------------- Build -------------------------------- */

        private String buildJsonString(Map<String, Map<String, Set<String>>> input) {
            List<String> pieces = new ArrayList<>();
            for (Map.Entry<String, Map<String, Set<String>>> e : input.entrySet()) {
                try {
                    String recipientsJson = toRecipientsJson(e.getValue());
                    pieces.add(String.format(Locale.ROOT, "\"%s\" : %s", e.getKey(), recipientsJson));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            return String.join(",", pieces);
        }

        public String build() {
            String allShares = buildJsonString(share);
            String allRevokes = buildJsonString(revoke);
            return String.format(Locale.ROOT, """
                {
                  "resource_id": "%s",
                  "resource_type": "%s",
                  "add": {
                    %s
                  },
                  "revoke": {
                    %s
                  }
                }
                """, configId, configType, allShares, allRevokes);
        }

        /* ------------------------------ Internals ------------------------------ */

        private static void mergeInto(
            Map<String, Map<String, Set<String>>> target,
            String accessLevel,
            Map<Recipient, Set<String>> incoming
        ) {
            if (incoming == null || incoming.isEmpty()) return;
            Map<String, Set<String>> existing = target.computeIfAbsent(accessLevel, k -> new HashMap<>());
            for (Map.Entry<Recipient, Set<String>> e : incoming.entrySet()) {
                if (e.getKey() == null) continue;
                if (e.getValue() == null || e.getValue().isEmpty()) continue;
                existing.computeIfAbsent(e.getKey().getName(), k -> new HashSet<>()).addAll(e.getValue());
            }
        }

        private static String toRecipientsJson(Map<String, Set<String>> recipients) throws IOException {
            if (recipients == null) {
                recipients = Collections.emptyMap();
            }

            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();

            for (Recipient recipient : Recipient.values()) {
                String key = recipient.getName();
                if (recipients.containsKey(key)) {
                    writeArray(builder, key, recipients.get(key));
                }
            }

            builder.endObject();
            return builder.toString();
        }

        private static void writeArray(XContentBuilder builder, String field, Set<String> values) throws IOException {
            builder.startArray(field);
            if (values != null) {
                for (String v : values) {
                    builder.value(v);
                }
            }
            builder.endArray();
        }
    }

    public static boolean isForbidden(Exception e) {
        if (e instanceof OpenSearchStatusException) {
            return ((OpenSearchStatusException) e).status() == RestStatus.FORBIDDEN;
        }
        if (e instanceof ResponseException) {
            return ((ResponseException) e).getResponse().getStatusLine().getStatusCode() == 403;
        }
        return false;
    }

    private static final Duration RS_WAIT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration RS_POLL_INTERVAL = Duration.ofMillis(200);

    // Core waiter: visible when the callable returns 200 OK; 403 means "not yet", anything else fails fast.
    private void waitUntilVisible(java.util.concurrent.Callable<Response> op) {
        Awaitility.await().atMost(RS_WAIT_TIMEOUT).pollInterval(RS_POLL_INTERVAL).until(() -> {
            try {
                Response r = op.call();
                return TestHelpers.restStatus(r) == RestStatus.OK;
            } catch (Exception e) {
                if (isForbidden(e)) {
                    // eventual consistency: not visible yet
                    return false;
                }
                // unexpected error: fail fast
                throw e;
            }
        });
    }

    // Core waiter: non-visible when the callable throws 403; 200 means "still visible", anything else fails fast.
    private void waitUntilForbidden(java.util.concurrent.Callable<Response> op) {
        Awaitility.await().atMost(RS_WAIT_TIMEOUT).pollInterval(RS_POLL_INTERVAL).until(() -> {
            try {
                op.call(); // 200 => still visible
                return false;
            } catch (Exception e) {
                if (isForbidden(e)) {
                    return true; // forbidden now
                }
                throw e; // unexpected error: fail fast
            }
        });
    }

    protected void waitForWorkflowSharingVisibility(String workflowId, RestClient client) {
        waitUntilVisible(() -> getWorkflow(client, workflowId));
    }

    protected void waitForWorkflowRevokeNonVisibility(String workflowId, RestClient client) {
        waitUntilForbidden(() -> getWorkflow(client, workflowId));
    }

    protected void waitForWorkflowStateSharingVisibility(String workflowId, RestClient client) {
        waitUntilVisible(() -> getWorkflowStatus(client, workflowId, false));
    }

    protected void waitForWorkflowStateRevokeNonVisibility(String workflowId, RestClient client) {
        waitUntilForbidden(() -> getWorkflowStatus(client, workflowId, false));
    }
}
