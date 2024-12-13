/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.rest.RestRequest;

import java.util.List;
import java.util.Map;

/**
 * Utility methods for Rest Handlers
 */
public class RestActionUtils {

    private RestActionUtils() {}

    /**
     * Finds the tenant id in the REST Headers
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @param restRequest the RestRequest
     * @return The tenant ID from the headers or null if multitenancy is not enabled
     */
    public static String getTenantID(Boolean isMultiTenancyEnabled, RestRequest restRequest) {
        if (isMultiTenancyEnabled) {
            Map<String, List<String>> headers = restRequest.getHeaders();
            if (headers.containsKey(CommonValue.TENANT_ID_HEADER)) {
                List<String> tenantIdList = headers.get(CommonValue.TENANT_ID_HEADER);
                if (tenantIdList != null && !tenantIdList.isEmpty()) {
                    String tenantId = tenantIdList.get(0);
                    if (tenantId != null) {
                        return tenantId;
                    } else {
                        throw new FlowFrameworkException("Tenant ID can't be null", RestStatus.FORBIDDEN);
                    }
                } else {
                    throw new FlowFrameworkException("Tenant ID header is present but has no value", RestStatus.FORBIDDEN);
                }
            } else {
                throw new FlowFrameworkException("Tenant ID header is missing", RestStatus.FORBIDDEN);
            }
        } else {
            return null;
        }
    }
}
