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
import org.opensearch.core.common.Strings;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

/**
 * Utility methods for Rest Handlers
 */
public class RestHandlerUtils {

    /** Fields that need to be excluded from the Search Response*/
    public static final String[] DASHBOARD_EXCLUDES = new String[] {
        CommonValue.USER_FIELD,
        CommonValue.UI_METADATA_FIELD,
        CommonValue.PATH_TO_CREDENTIAL_FIELD };

    public static final String[] EXCLUDES = new String[] { CommonValue.USER_FIELD, CommonValue.PATH_TO_CREDENTIAL_FIELD };

    private RestHandlerUtils() {}

    /**
     * Creates a source context and include/exclude information to be shared based on the user
     *
     * @param request the REST request
     * @param searchSourceBuilder the search request source builder
     * @return modified sources
     */
    public static FetchSourceContext getSourceContext(RestRequest request, SearchSourceBuilder searchSourceBuilder) {
        // TODO
        // 1. check if the request came from dashboard and exclude UI_METADATA
        if (searchSourceBuilder.fetchSource() != null) {
            String[] newArray = (String[]) ArrayUtils.addAll(searchSourceBuilder.fetchSource().excludes(), DASHBOARD_EXCLUDES);
            return new FetchSourceContext(true, searchSourceBuilder.fetchSource().includes(), newArray);
        } else {
            // When user does not set the _source field in search api request, searchSourceBuilder.fetchSource becomes null
            return new FetchSourceContext(true, Strings.EMPTY_ARRAY, EXCLUDES);
        }
    }
}
