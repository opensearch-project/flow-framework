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
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;

public class RestHandlerUtils {

    public static final String[] USER_EXCLUDE = new String[] { CommonValue.USER_FIELD };

    private RestHandlerUtils() {}

    public static FetchSourceContext getSourceContext(RestRequest request, SearchSourceBuilder searchSourceBuilder) {
        // TODO
        // 1. Move UI_METADATA to GC Index
        // 2. check if the request came from dashboard and exclude UI_METADATA
        if (searchSourceBuilder.fetchSource() != null) {
            String[] newArray = (String[]) ArrayUtils.addAll(searchSourceBuilder.fetchSource().excludes(), USER_EXCLUDE);
            return new FetchSourceContext(true, searchSourceBuilder.fetchSource().includes(), newArray);
        } else {
            return null;
        }
    }
}
