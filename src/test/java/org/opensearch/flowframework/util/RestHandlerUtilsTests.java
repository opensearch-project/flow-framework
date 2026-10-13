/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.commons.authuser.User;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RestHandlerUtilsTests extends OpenSearchTestCase {

    public void testGetSourceContextFromClientWithDashboardExcludes() {
        SearchSourceBuilder testSearchSourceBuilder = new SearchSourceBuilder();
        testSearchSourceBuilder.fetchSource(new String[] { "a" }, new String[] { "b" });
        User user = new User("user", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        FetchSourceContext sourceContext = RestHandlerUtils.getSourceContext(user, testSearchSourceBuilder);
        assertEquals(sourceContext.excludes().length, 4);
    }

    public void testGetSourceContextFromClientWithExcludes() {
        SearchSourceBuilder testSearchSourceBuilder = new SearchSourceBuilder();
        User user = new User("user", Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        FetchSourceContext sourceContext = RestHandlerUtils.getSourceContext(user, testSearchSourceBuilder);
        assertEquals(sourceContext.excludes().length, 2);
    }

    public void testGetSourceContextAdminUser() {
        SearchSourceBuilder testSearchSourceBuilder = new SearchSourceBuilder();
        List<String> roles = new ArrayList<>();
        roles.add("all_access");

        User user = new User("admin", roles, roles, Collections.emptyList());
        FetchSourceContext sourceContext = RestHandlerUtils.getSourceContext(user, testSearchSourceBuilder);
        assertEquals(sourceContext.excludes().length, 1);
    }

}
