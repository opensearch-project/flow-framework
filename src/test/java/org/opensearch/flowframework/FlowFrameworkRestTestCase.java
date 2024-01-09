/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
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
import org.junit.AfterClass;
import org.junit.Before;

import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE;
import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_TOTAL;
import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_ENABLED;
import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH;
import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_KEYPASSWORD;
import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_PASSWORD;
import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_SSL_HTTP_PEMCERT_FILEPATH;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_URI;

/**
 * Base rest integration test class, supports security enabled/disabled cluster
 */
public abstract class FlowFrameworkRestTestCase extends OpenSearchRestTestCase {

    @Before
    public void setUpSettings() throws Exception {

        if (!indexExistsWithAdminClient(".plugins-ml-config")) {

            // Initial cluster set up

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

            // Enable ML Commons to run on non-ml nodes
            response = TestHelpers.makeRequest(
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

            // Ensure .plugins-ml-config is created before proceeding with integration tests
            assertBusy(() -> { assertTrue(indexExistsWithAdminClient(".plugins-ml-config")); }, 60, TimeUnit.SECONDS);
        }

    }

    protected boolean isHttps() {
        boolean isHttps = Optional.ofNullable(System.getProperty("https")).map("true"::equalsIgnoreCase).orElse(false);
        if (isHttps) {
            // currently only external cluster is supported for security enabled testing
            if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
                throw new RuntimeException("cluster url should be provided for security enabled testing");
            }
        }

        return isHttps;
    }

    @Override
    protected Settings restClientSettings() {
        return super.restClientSettings();
    }

    @Override
    protected String getProtocol() {
        return isHttps() ? "https" : "http";
    }

    @Override
    protected Settings restAdminSettings() {
        return Settings.builder()
            // disable the warning exception for admin client since it's only used for cleanup.
            .put("strictDeprecationMode", false)
            .put("http.port", 9200)
            .put(OPENSEARCH_SECURITY_SSL_HTTP_ENABLED, isHttps())
            .put(OPENSEARCH_SECURITY_SSL_HTTP_PEMCERT_FILEPATH, "sample.pem")
            .put(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH, "test-kirk.jks")
            .put(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_PASSWORD, "changeit")
            .put(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_KEYPASSWORD, "changeit")
            .build();
    }

    // Utility fn for deleting indices. Should only be used when not allowed in a regular context
    // (e.g., deleting system indices)
    protected static void deleteIndexWithAdminClient(String name) throws IOException {
        Request request = new Request("DELETE", "/" + name);
        adminClient().performRequest(request);
    }

    // Utility fn for checking if an index exists. Should only be used when not allowed in a regular context
    // (e.g., checking existence of system indices)
    protected static boolean indexExistsWithAdminClient(String indexName) throws IOException {
        Request request = new Request("HEAD", "/" + indexName);
        Response response = adminClient().performRequest(request);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        boolean strictDeprecationMode = settings.getAsBoolean("strictDeprecationMode", true);
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isHttps()) {
            String keystore = settings.get(OPENSEARCH_SECURITY_SSL_HTTP_KEYSTORE_FILEPATH);
            if (Objects.nonNull(keystore)) {
                URI uri = null;
                try {
                    uri = this.getClass().getClassLoader().getResource("security/sample.pem").toURI();
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
                Path configPath = PathUtils.get(uri).getParent().toAbsolutePath();
                return new SecureRestClientBuilder(settings, configPath).build();
            } else {
                configureHttpsClient(builder, settings);
                builder.setStrictDeprecationMode(strictDeprecationMode);
                return builder.build();
            }

        } else {
            configureClient(builder, settings);
            builder.setStrictDeprecationMode(strictDeprecationMode);
            return builder.build();
        }

    }

    // Cleans up resources after all test execution has been completed
    @SuppressWarnings("unchecked")
    @AfterClass
    protected static void wipeAllSystemIndices() throws IOException {
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
                if (indexName != null && !".opendistro_security".equals(indexName)) {
                    adminClient().performRequest(new Request("DELETE", "/" + indexName));
                }
            }
        }
    }

    protected static void configureHttpsClient(RestClientBuilder builder, Settings settings) throws IOException {
        Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
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
                    .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                        @Override
                        public TlsDetails create(final SSLEngine sslEngine) {
                            return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                        }
                    })
                    .build();
                final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                    .setMaxConnPerRoute(DEFAULT_MAX_CONN_PER_ROUTE)
                    .setMaxConnTotal(DEFAULT_MAX_CONN_TOTAL)
                    .setTlsStrategy(tlsStrategy)
                    .build();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(conf -> {
            Timeout timeout = Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis()));
            conf.setConnectTimeout(timeout);
            conf.setResponseTimeout(timeout);
            return conf;
        });
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
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
     * Helper method to invoke the Create Workflow Rest Action without validation
     * @param template the template to create
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflow(Template template) throws Exception {
        return TestHelpers.makeRequest(client(), "POST", WORKFLOW_URI + "?validation=off", Collections.emptyMap(), template.toJson(), null);
    }

    /**
     * Helper method to invoke the Create Workflow Rest Action with provision
     * @param template the template to create
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflowWithProvision(Template template) throws Exception {
        return TestHelpers.makeRequest(client(), "POST", WORKFLOW_URI + "?provision=true", Collections.emptyMap(), template.toJson(), null);
    }

    /**
     * Helper method to invoke the Create Workflow Rest Action with validation
     * @param template the template to create
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response createWorkflowValidation(Template template) throws Exception {
        return TestHelpers.makeRequest(client(), "POST", WORKFLOW_URI, Collections.emptyMap(), template.toJson(), null);
    }

    /**
     * Helper method to invoke the Update Workflow API
     * @param workflowId the document id
     * @param template the template used to update
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response updateWorkflow(String workflowId, Template template) throws Exception {
        return TestHelpers.makeRequest(
            client(),
            "PUT",
            String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, workflowId),
            Collections.emptyMap(),
            template.toJson(),
            null
        );
    }

    /**
     * Helper method to invoke the Provision Workflow Rest Action
     * @param workflowId the workflow ID to provision
     * @throws Exception if the request fails
     * @return a rest response
     */
    protected Response provisionWorkflow(String workflowId) throws Exception {
        return TestHelpers.makeRequest(
            client(),
            "POST",
            String.format(Locale.ROOT, "%s/%s/%s", WORKFLOW_URI, workflowId, "_provision"),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Deprovision Workflow Rest Action
     * @param workflowId the workflow ID to deprovision
     * @return a rest response
     * @throws Exception if the request fails
     */
    protected Response deprovisionWorkflow(String workflowId) throws Exception {
        return TestHelpers.makeRequest(
            client(),
            "POST",
            String.format(Locale.ROOT, "%s/%s/%s", WORKFLOW_URI, workflowId, "_deprovision"),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Delete Workflow Rest Action
     * @param workflowId the workflow ID to delete
     * @return a rest response
     * @throws Exception if the request fails
     */
    protected Response deleteWorkflow(String workflowId) throws Exception {
        return TestHelpers.makeRequest(
            client(),
            "DELETE",
            String.format(Locale.ROOT, "%s/%s", WORKFLOW_URI, workflowId),
            Collections.emptyMap(),
            "",
            null
        );
    }

    /**
     * Helper method to invoke the Get Workflow Rest Action
     * @param workflowId the workflow ID to get the status
     * @param all verbose status flag
     * @throws Exception if the request fails
     * @return rest response
     */
    protected Response getWorkflowStatus(String workflowId, boolean all) throws Exception {
        return TestHelpers.makeRequest(
            client(),
            "GET",
            String.format(Locale.ROOT, "%s/%s/%s?all=%s", WORKFLOW_URI, workflowId, "_status", all),
            Collections.emptyMap(),
            "",
            null
        );

    }

    protected Response getWorkflowStep() throws Exception {
        return TestHelpers.makeRequest(
                client(),
                "GET",
                String.format("%s/%s", WORKFLOW_URI, "_step"),
                Collections.emptyMap(),
                "",
                null
        );
    }

    /**
     * Helper method to invoke the Search Workflow Rest Action with the given query
     * @param query the search query
     * @return rest response
     * @throws Exception if the request fails
     */
    protected SearchResponse searchWorkflows(String query) throws Exception {

        // Execute search
        Response restSearchResponse = TestHelpers.makeRequest(
            client(),
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

    protected SearchResponse searchWorkflowState(String query) throws Exception {
        Response restSearchResponse = TestHelpers.makeRequest(
            client(),
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
     * Helper method to invoke the Get Workflow Rest Action and assert the provisioning and state status
     * @param workflowId the workflow ID to get the status
     * @param stateStatus the state status name
     * @param provisioningStatus the provisioning status name
     * @throws Exception if the request fails
     */
    protected void getAndAssertWorkflowStatus(String workflowId, State stateStatus, ProvisioningProgress provisioningStatus)
        throws Exception {
        Response response = getWorkflowStatus(workflowId, true);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));

        Map<String, Object> responseMap = entityAsMap(response);
        assertEquals(stateStatus.name(), (String) responseMap.get(CommonValue.STATE_FIELD));
        assertEquals(provisioningStatus.name(), (String) responseMap.get(CommonValue.PROVISIONING_PROGRESS_FIELD));

    }

    protected void getAndAssertWorkflowStep() throws Exception {
        Response response = getWorkflowStep();
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    /**
     * Helper method to wait until a workflow provisioning has completed and retrieve any resources created
     * @param workflowId the workflow id to retrieve resources from
     * @param timeout the max wait time in seconds
     * @return a list of created resources
     * @throws Exception if the request fails
     */
    protected List<ResourceCreated> getResourcesCreated(String workflowId, int timeout) throws Exception {

        // wait and ensure state is completed/done
        assertBusy(
            () -> { getAndAssertWorkflowStatus(workflowId, State.COMPLETED, ProvisioningProgress.DONE); },
            timeout,
            TimeUnit.SECONDS
        );

        Response response = getWorkflowStatus(workflowId, true);

        // Parse workflow state from response and retreieve resources created
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
}
