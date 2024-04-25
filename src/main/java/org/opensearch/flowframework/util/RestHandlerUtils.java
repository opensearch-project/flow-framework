/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.apache.commons.lang3.ArrayUtils;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.Strings;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * Utility methods for Rest Handlers
 */
public class RestHandlerUtils {

    /** Path to credential field **/
    private static final String PATH_TO_CREDENTIAL_FIELD = "workflows.provision.nodes.user_inputs.credential";

    /** Fields that need to be excluded from the Search Response*/
    private static final String[] DASHBOARD_EXCLUDES = new String[] {
        CommonValue.USER_FIELD,
        CommonValue.UI_METADATA_FIELD,
        PATH_TO_CREDENTIAL_FIELD };

    private static final String[] EXCLUDES = new String[] { CommonValue.USER_FIELD, PATH_TO_CREDENTIAL_FIELD };

    private RestHandlerUtils() {}

    /**
     * Creates a source context and include/exclude information to be shared based on the user
     *
     * @param user User
     * @param searchSourceBuilder the search request source builder
     * @return modified sources
     */
    public static FetchSourceContext getSourceContext(User user, SearchSourceBuilder searchSourceBuilder) {
        if (searchSourceBuilder.fetchSource() != null) {
            String[] newArray = (String[]) ArrayUtils.addAll(searchSourceBuilder.fetchSource().excludes(), DASHBOARD_EXCLUDES);
            return new FetchSourceContext(true, searchSourceBuilder.fetchSource().includes(), newArray);
        } else {
            // When user does not set the _source field in search api request, searchSourceBuilder.fetchSource becomes null
            if (ParseUtils.isAdmin(user)) {
                return new FetchSourceContext(true, Strings.EMPTY_ARRAY, new String[] { PATH_TO_CREDENTIAL_FIELD });
            }
            return new FetchSourceContext(true, Strings.EMPTY_ARRAY, EXCLUDES);
        }
    }
}
