/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.transport.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.util.Arrays;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.flowframework.util.ParseUtils.isAdmin;
import static org.opensearch.flowframework.util.RestHandlerUtils.getSourceContext;

/**
 * Handle general search request, check user role and return search response.
 */
public class SearchHandler {
    private final Logger logger = LogManager.getLogger(SearchHandler.class);
    private final Client client;
    private final SdkClient sdkClient;
    private volatile Boolean filterByBackendRole;

    /**
     * Instantiates a new SearchHandler
     * @param settings settings
     * @param clusterService cluster service
     * @param client The node client to retrieve a stored use case template
     * @param sdkClient The multitenant client
     * @param filterByBackendRoleSetting filter role backend settings
     */
    public SearchHandler(
        Settings settings,
        ClusterService clusterService,
        Client client,
        SdkClient sdkClient,
        Setting<Boolean> filterByBackendRoleSetting
    ) {
        this.client = client;
        this.sdkClient = sdkClient;
        filterByBackendRole = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterByBackendRole = it);
    }

    /**
     * Search workflows in global context
     * @param request SearchRequest
     * @param tenantId the tenant ID
     * @param actionListener ActionListener
     */
    public void search(SearchRequest request, String tenantId, ActionListener<SearchResponse> actionListener) {
        // AccessController should take care of letting the user with right permission to view the workflow
        User user = ParseUtils.getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            logger.info("Searching workflows in global context");
            SearchSourceBuilder searchSourceBuilder = request.source();
            searchSourceBuilder.fetchSource(getSourceContext(user, searchSourceBuilder));
            validateRole(request, tenantId, user, actionListener, context);
        } catch (Exception e) {
            logger.error("Failed to search workflows in global context", e);
            actionListener.onFailure(e);
        }
    }

    /**
     * Validate user role and call search
     * @param request SearchRequest
     * @param tenantId the tenant id
     * @param user User
     * @param listener ActionListener
     * @param context ThreadContext
     */
    public void validateRole(
        SearchRequest request,
        String tenantId,
        User user,
        ActionListener<SearchResponse> listener,
        ThreadContext.StoredContext context
    ) {
        if (user == null || !filterByBackendRole || isAdmin(user)) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            // case 3: user is admin which means we don't have to check backend role filtering
            doSearch(request, tenantId, ActionListener.runBefore(listener, context::restore));
        } else {
            // Security is enabled, filter is enabled and user isn't admin
            try {
                ParseUtils.addUserBackendRolesFilter(user, request.source());
                logger.debug("Filtering result by {}", user.getBackendRoles());
                doSearch(request, tenantId, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private void doSearch(SearchRequest request, String tenantId, ActionListener<SearchResponse> listener) {
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(request.indices())
            .tenantId(tenantId)
            .searchSourceBuilder(request.source())
            .build();
        sdkClient.searchDataObjectAsync(searchRequest).whenComplete((r, throwable) -> {
            if (throwable == null) {
                try {
                    SearchResponse searchResponse = SearchResponse.fromXContent(r.parser());
                    logger.info(Arrays.toString(request.indices()) + " search complete: {}", searchResponse.getHits().getTotalHits());
                    listener.onResponse(searchResponse);
                } catch (Exception e) {
                    logger.error("Failed to parse search response", e);
                    listener.onFailure(new FlowFrameworkException("Failed to parse search response", INTERNAL_SERVER_ERROR));
                }
            } else {
                Exception cause = SdkClientUtils.unwrapAndConvertToException(throwable);
                logger.error("Search failed for indices: {}", Arrays.toString(request.indices()), cause);
                listener.onFailure(cause);
            }
        });
    }
}
