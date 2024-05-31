/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.model.ProvisioningProgress;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.flowframework.model.State;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.model.WorkflowState;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

/**
 * Base rest integration test class, supports security enabled/disabled cluster
 */
public abstract class FlowFrameworkRestTestCase extends OpenSearchRestTestCase {

    private static String FLOW_FRAMEWORK_FULL_ACCESS_ROLE = "flow_framework_full_access";
    private static String ML_COMMONS_FULL_ACCESS_ROLE = "ml_full_access";
    private static String READ_ACCESS_ROLE = "flow_framework_read_access";
    public static String FULL_ACCESS_USER = "fullAccessUser";
    public static String READ_ACCESS_USER = "readAccessUser";
    private static RestClient readAccessClient;
    private static RestClient fullAccessClient;

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

        // Set up clients if running in security enabled cluster
        if (isHttps()) {
            String fullAccessUserPassword = generatePassword(FULL_ACCESS_USER);
            String readAccessUserPassword = generatePassword(READ_ACCESS_USER);

            // Configure full access user and client, needs ML Full Access role as well
            response = createUser(
                FULL_ACCESS_USER,
                fullAccessUserPassword,
                List.of(FLOW_FRAMEWORK_FULL_ACCESS_ROLE, ML_COMMONS_FULL_ACCESS_ROLE)
            );
            fullAccessClient = new SecureRestClientBuilder(
                getClusterHosts().toArray(new HttpHost[0]),
                isHttps(),
                FULL_ACCESS_USER,
                fullAccessUserPassword
            ).setSocketTimeout(60000).build();

            // Configure read access user and client
            response = createUser(READ_ACCESS_USER, readAccessUserPassword, List.of(READ_ACCESS_ROLE));
            readAccessClient = new SecureRestClientBuilder(
                getClusterHosts().toArray(new HttpHost[0]),
                isHttps(),
                READ_ACCESS_USER,
                readAccessUserPassword
            ).setSocketTimeout(60000).build();
        }

    }

    protected static RestClient fullAccessClient() {
        return fullAccessClient;
    }

    protected static RestClient readAccessClient() {
        return readAccessClient;
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
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            try {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    // disable the certificate since our testing cluster just uses the default security configuration
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSSLContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build());
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
        builder.setRequestConfigCallback(conf -> {
            int timeout = Math.toIntExact(socketTimeout.getMillis());
            conf.setConnectTimeout(timeout);
            conf.setSocketTimeout(timeout);
            return conf;
        });
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    @SuppressWarnings("unchecked")
    @After
    protected void wipeAllODFEIndices() throws IOException {
        Response response = adminClient().performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
        MediaType xContentType = MediaType.fromMediaType(response.getEntity().getContentType().getValue());
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
     * Create an unique password. Simple password are weak due to https://tinyurl.com/383em9zk
     * @return a random password.
     */
    public static String generatePassword(String username) {
        return RandomStringUtils.random(15, true, true);
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
     * @param the required params
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflowWithUseCase(RestClient client, String useCase, List<String> params) throws Exception {

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
        MediaType mediaType = MediaType.fromMediaType(restSearchResponse.getEntity().getContentType().getValue());
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
        MediaType mediaType = MediaType.fromMediaType(restSearchResponse.getEntity().getContentType().getValue());
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
        assertEquals(stateStatus.name(), responseMap.get(CommonValue.STATE_FIELD));
        assertEquals(provisioningStatus.name(), responseMap.get(CommonValue.PROVISIONING_PROGRESS_FIELD));
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

        Response response = getWorkflowStatus(client, workflowId, true);

        // Parse workflow state from response and retrieve resources created
        MediaType mediaType = MediaType.fromMediaType(response.getEntity().getContentType().getValue());
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

    protected Response createUser(String name, String password, List<String> backendRoles) throws IOException {
        String backendRolesString = backendRoles.stream().map(item -> "\"" + item + "\"").collect(Collectors.joining(","));
        String json = "{\"password\": \""
            + password
            + "\",\"opendistro_security_roles\": ["
            + backendRolesString
            + "],\"backend_roles\": [],\"attributes\": {}}";
        return TestHelpers.makeRequest(
            client(),
            "PUT",
            "/_opendistro/_security/api/internalusers/" + name,
            null,
            TestHelpers.toHttpEntity(json),
            null
        );
    }

    protected Response deleteUser(String user) throws IOException {
        return TestHelpers.makeRequest(client(), "DELETE", "/_opendistro/_security/api/internalusers/" + user, null, "", null);
    }

    protected GetPipelineResponse getPipelines(String pipelineId) throws IOException {
        Response getPipelinesResponse = TestHelpers.makeRequest(client(), "GET", "_ingest/pipeline/" + pipelineId, null, "", null);

        // Parse entity content into SearchResponse
        MediaType mediaType = MediaType.fromMediaType(getPipelinesResponse.getEntity().getContentType().getValue());
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

    @SuppressWarnings("unchecked")
    protected List<String> catPlugins() throws IOException {
        Response response = TestHelpers.makeRequest(
            client(),
            "GET",
            "_cat/plugins?s=component&h=name,component,version,description&format=json",
            null,
            "",
            List.of(new BasicHeader(HttpHeaders.USER_AGENT, ""))
        );
        List<Object> pluginsList = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            response.getEntity().getContent()
        ).list();
        return pluginsList.stream().map(o -> ((Map<String, Object>) o).get("component").toString()).collect(Collectors.toList());
    }
}
