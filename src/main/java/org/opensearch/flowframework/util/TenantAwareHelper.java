/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.CommonValue;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.transport.WorkflowResponse;
import org.opensearch.rest.RestRequest;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class for tenant ID validation
 */
public class TenantAwareHelper {

    private static final ConcurrentHashMap<String, AtomicInteger> activeProvisionsPerTenant = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicInteger> activeDeprovisionsPerTenant = new ConcurrentHashMap<>();

    private TenantAwareHelper() {}

    /**
     * Validates the tenant ID based on the multi-tenancy feature setting.
     *
     * @param isMultiTenancyEnabled whether the multi-tenancy feature is enabled.
     * @param tenantId The tenant ID to validate.
     * @param listener The action listener to handle failure cases.
     * @return true if the tenant ID is valid or if multi-tenancy is not enabled; false if the tenant ID is invalid and multi-tenancy is enabled.
     */
    public static boolean validateTenantId(boolean isMultiTenancyEnabled, String tenantId, ActionListener<?> listener) {
        if (isMultiTenancyEnabled && tenantId == null) {
            listener.onFailure(new FlowFrameworkException("No permission to access this resource", RestStatus.FORBIDDEN));
            return false;
        } else {
            return true;
        }
    }

    /**
     * Validates the tenant resource by comparing the tenant ID from the request with the tenant ID from the resource.
     *
     * @param isMultiTenancyEnabled whether the multi-tenancy feature is enabled.
     * @param tenantIdFromRequest The tenant ID obtained from the request.
     * @param tenantIdFromResource The tenant ID obtained from the resource.
     * @param listener The action listener to handle failure cases.
     * @return true if the tenant IDs match or if multi-tenancy is not enabled; false if the tenant IDs do not match and multi-tenancy is enabled.
     */
    public static boolean validateTenantResource(
        boolean isMultiTenancyEnabled,
        String tenantIdFromRequest,
        String tenantIdFromResource,
        ActionListener<?> listener
    ) {
        if (isMultiTenancyEnabled && !Objects.equals(tenantIdFromRequest, tenantIdFromResource)) {
            listener.onFailure(new FlowFrameworkException("No permission to access this resource", RestStatus.FORBIDDEN));
            return false;
        } else return true;
    }

    /**
     * Finds the tenant id in the REST Headers
     * @param isMultiTenancyEnabled whether multitenancy is enabled
     * @param restRequest the RestRequest
     * @return The tenant ID from the headers or null if multitenancy is not enabled
     */
    public static String getTenantID(Boolean isMultiTenancyEnabled, RestRequest restRequest) {
        if (!isMultiTenancyEnabled) {
            return null;
        }

        Map<String, List<String>> headers = restRequest.getHeaders();

        List<String> tenantIdList = headers.get(CommonValue.TENANT_ID_HEADER);
        if (tenantIdList == null || tenantIdList.isEmpty()) {
            throw new FlowFrameworkException("Tenant ID header is missing or has no value", RestStatus.FORBIDDEN);
        }

        String tenantId = tenantIdList.get(0);
        if (tenantId == null) {
            throw new FlowFrameworkException("Tenant ID can't be null", RestStatus.FORBIDDEN);
        }

        return tenantId;
    }

    /**
     * Attempts to acquire a provision slot for the given tenant.
     *
     * @param maxExecutions The maximum number of simultaneous provisions allowed per tenant.
     * @param tenantId The ID of the tenant requesting the provision.
     * @param workflowListener The listener to notify in case of failure.
     * @return true if the provision slot was acquired, false otherwise.
     */
    public static boolean tryAcquireProvision(int maxExecutions, String tenantId, ActionListener<WorkflowResponse> workflowListener) {
        if (!tryAcquire(tenantId, activeProvisionsPerTenant, maxExecutions)) {
            workflowListener.onFailure(
                new FlowFrameworkException(
                    "Exceeded max simultaneous provisioning requests: " + maxExecutions,
                    RestStatus.TOO_MANY_REQUESTS
                )
            );
            return false;
        }
        return true;
    }

    /**
     * Attempts to acquire a deprovision slot for the given tenant.
     *
     * @param maxExecutions The maximum number of simultaneous deprovisions allowed per tenant.
     * @param tenantId The ID of the tenant requesting the deprovision.
     * @param workflowListener The listener to notify in case of failure.
     * @return true if the deprovision slot was acquired, false otherwise.
     */
    public static boolean tryAcquireDeprovision(int maxExecutions, String tenantId, ActionListener<WorkflowResponse> workflowListener) {
        if (!tryAcquire(tenantId, activeDeprovisionsPerTenant, maxExecutions)) {
            workflowListener.onFailure(
                new FlowFrameworkException(
                    "Exceeded max simultaneous deprovisioning requests: " + maxExecutions,
                    RestStatus.TOO_MANY_REQUESTS
                )
            );
            return false;
        }
        return true;
    }

    /**
     * Releases a provision slot for the given tenant.
     *
     * @param tenantId The ID of the tenant for which to release the provision slot.
     */
    public static void releaseProvision(String tenantId) {
        release(tenantId, activeProvisionsPerTenant);
    }

    /**
     * Releases a deprovision slot for the given tenant.
     *
     * @param tenantId The ID of the tenant for which to release the deprovision slot.
     */
    public static void releaseDeprovision(String tenantId) {
        release(tenantId, activeDeprovisionsPerTenant);
    }

    /**
     * Attempts to acquire an execution slot for the given tenant.
     *
     * @param tenantId The ID of the tenant requesting the execution slot.
     * @param executionsMap The map tracking the number of active executions per tenant.
     * @param maxExecutions The maximum number of simultaneous executions allowed per tenant.
     * @return true if the execution slot was acquired, false otherwise.
     */
    private static boolean tryAcquire(String tenantId, ConcurrentHashMap<String, AtomicInteger> executionsMap, int maxExecutions) {
        if (tenantId == null) {
            return true; // No throttling for null tenantId
        }
        AtomicInteger count = executionsMap.computeIfAbsent(tenantId, k -> new AtomicInteger(0));
        if (count.incrementAndGet() <= maxExecutions) {
            return true;
        } else {
            // If we're here we have exceeded maxExecutions, roll back
            count.decrementAndGet();
            return false;
        }
    }

    /**
     * Releases an execution slot for the given tenant.
     *
     * @param tenantId The ID of the tenant for which to release the execution slot.
     * @param executionsMap The map tracking the number of active executions per tenant.
     */
    private static void release(String tenantId, ConcurrentHashMap<String, AtomicInteger> executionsMap) {
        if (tenantId == null) {
            return; // No throttling for null tenantId
        }
        executionsMap.computeIfPresent(tenantId, (key, count) -> {
            int newValue = count.decrementAndGet();
            // returning null from computeIfPresent removes from the map
            return newValue > 0 ? count : null;
        });
    }

    /**
     * Create an action listener that releases provision throttle on failure only
     * @param tenantId The tenant ID
     * @param delegate the wrapped listener
     * @return a listener wrapping the delegate that calls {@link #releaseProvision(String)} on failure of the wrapped listener.
     */
    public static ActionListener<WorkflowResponse> releaseProvisionOnFailureListener(
        String tenantId,
        ActionListener<WorkflowResponse> delegate
    ) {
        // Since this is only called on async failure we can't use runBefore, so we just copy its code in one case
        return ActionListener.notifyOnce(new ActionListener<>() {
            @Override
            public void onResponse(WorkflowResponse response) {
                delegate.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    releaseProvision(tenantId);
                } finally {
                    delegate.onFailure(e);
                }
            }
        });
    }

    /**
     * Create an action listener that releases deprovision throttle
     * @param tenantId The tenant ID
     * @param delegate the wrapped listener
     * @return a listener wrapping the delegate that calls {@link #releaseDeprovision(String)} on the wrapped listener.
     */
    public static ActionListener<WorkflowResponse> releaseDeprovisionListener(String tenantId, ActionListener<WorkflowResponse> delegate) {
        // Since this is only called on synchronous calls we release on both success and failure
        return ActionListener.notifyOnce(ActionListener.runBefore(delegate, () -> releaseDeprovision(tenantId)));
    }
}
