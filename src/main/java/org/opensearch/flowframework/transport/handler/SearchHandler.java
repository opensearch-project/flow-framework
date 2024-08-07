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
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.search.builder.SearchSourceBuilder;

import static org.opensearch.flowframework.util.ParseUtils.isAdmin;
import static org.opensearch.flowframework.util.RestHandlerUtils.getSourceContext;

public class SearchHandler {
    private final Logger logger = LogManager.getLogger(SearchHandler.class);
    private final Client client;
    private volatile Boolean filterEnabled;

    public SearchHandler(Settings settings, ClusterService clusterService, Client client, Setting<Boolean> filterByBackendRoleSetting) {
        this.client = client;
        filterEnabled = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterEnabled = it);
    }

    public void search(SearchRequest request, ActionListener<SearchResponse> actionListener) {
        // AccessController should take care of letting the user with right permission to view the workflow
        User user = ParseUtils.getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            logger.info("Searching workflows in global context");
            SearchSourceBuilder searchSourceBuilder = request.source();
            searchSourceBuilder.fetchSource(getSourceContext(user, searchSourceBuilder));
            validateRole(request, user, actionListener, context);
        } catch (Exception e) {
            logger.error("Failed to search workflows in global context", e);
            actionListener.onFailure(e);
        }
    }

    public void validateRole(
        SearchRequest request,
        User user,
        ActionListener<SearchResponse> listener,
        ThreadContext.StoredContext context
    ) {
        if (user == null || !filterEnabled || isAdmin(user)) {
            // Case 1: user == null when 1. Security is disabled. 2. When user is super-admin
            // Case 2: If Security is enabled and filter is disabled, proceed with search as
            // user is already authenticated to hit this API.
            // case 3: user is admin which means we don't have to check backend role filtering
            client.search(request, ActionListener.runBefore(listener, context::restore));
        } else {
            // Security is enabled, filter is enabled and user isn't admin
            try {
                ParseUtils.addUserBackendRolesFilter(user, request.source());
                logger.debug("Filtering result by " + user.getBackendRoles());
                client.search(request, ActionListener.runBefore(listener, context::restore));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
