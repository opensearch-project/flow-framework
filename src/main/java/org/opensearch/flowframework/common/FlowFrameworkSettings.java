/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;

/** The common settings of flow framework  */
public class FlowFrameworkSettings {

    private volatile Boolean isFlowFrameworkEnabled;
    /** The duration between request retries */
    private volatile TimeValue retryDuration;
    /** Max workflow steps that can be created */
    private volatile Integer maxWorkflowSteps;
    /** Max workflows that can be created*/
    protected volatile Integer maxWorkflows;
    /** Timeout for internal requests*/
    protected volatile TimeValue requestTimeout;
    /** Whether multitenancy is enabled */
    private final Boolean isMultiTenancyEnabled;
    /** Size of the threadpool used for retryable tasks in workflows */
    private volatile Integer workflowThreadPoolSize;
    /** Size of the threadpool for provisioning */
    private volatile Integer provisionThreadPoolSize;
    /** Max simultaneous provision requests */
    private volatile Integer maxActiveProvisionsPerTenant;
    /** Size of the threadpool for deprovisioning */
    private volatile Integer deprovisionThreadPoolSize;
    /** Max simultaneous deprovision requests */
    private volatile Integer maxActiveDeprovisionsPerTenant;

    /** The upper limit of max workflows that can be created  */
    public static final int MAX_WORKFLOWS_LIMIT = 10000;
    /** The upper limit of max workflow steps that can be in a single workflow  */
    public static final int MAX_WORKFLOW_STEPS_LIMIT = 500;

    /** This setting sets max workflows that can be created */
    public static final Setting<Integer> MAX_WORKFLOWS = Setting.intSetting(
        "plugins.flow_framework.max_workflows",
        1000,
        0,
        MAX_WORKFLOWS_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting sets max workflows that can be created */
    public static final Setting<Integer> MAX_WORKFLOW_STEPS = Setting.intSetting(
        "plugins.flow_framework.max_workflow_steps",
        50,
        1,
        MAX_WORKFLOW_STEPS_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting sets the timeout for the request */
    public static final Setting<TimeValue> WORKFLOW_REQUEST_TIMEOUT = Setting.positiveTimeSetting(
        "plugins.flow_framework.request_timeout",
        TimeValue.timeValueSeconds(10),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting enables/disables the Flow Framework REST API */
    public static final Setting<Boolean> FLOW_FRAMEWORK_ENABLED = Setting.boolSetting(
        "plugins.flow_framework.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting sets the time between task request retries */
    public static final Setting<TimeValue> TASK_REQUEST_RETRY_DURATION = Setting.positiveTimeSetting(
        "plugins.flow_framework.task_request_retry_duration",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting sets the backend role filtering */
    public static final Setting<Boolean> FILTER_BY_BACKEND_ROLES = Setting.boolSetting(
        "plugins.flow_framework.filter_by_backend_roles",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Indicates whether multi-tenancy is enabled in Flow Framework.
     *
     * This is a static setting that must be configured before starting OpenSearch. The corresponding setting {@code plugins.ml_commons.multi_tenancy_enabled} in the ML Commons plugin should match.
     *
     * It can be set in the following ways, in priority order:
     *
     * <ol>
     *   <li>As a command-line argument using the <code>-E</code> flag (this overrides other options):
     *       <pre>
     *       ./bin/opensearch -Eplugins.flow_framework.multi_tenancy_enabled=true
     *       </pre>
     *   </li>
     *   <li>As a system property using <code>OPENSEARCH_JAVA_OPTS</code> (this overrides <code>opensearch.yml</code>):
     *       <pre>
     *       export OPENSEARCH_JAVA_OPTS="-Dplugins.flow_framework.multi_tenancy_enabled=true"
     *       ./bin/opensearch
     *       </pre>
     *       Or inline when starting OpenSearch:
     *       <pre>
     *       OPENSEARCH_JAVA_OPTS="-Dplugins.flow_framework.multi_tenancy_enabled=true" ./bin/opensearch
     *       </pre>
     *   </li>
     *   <li>In the <code>opensearch.yml</code> configuration file:
     *       <pre>
     *       plugins.flow_framework.multi_tenancy_enabled: true
     *       </pre>
     *   </li>
     * </ol>
     *
     * After setting this option, a full cluster restart is required for the changes to take effect.
     */
    public static final Setting<Boolean> FLOW_FRAMEWORK_MULTI_TENANCY_ENABLED = Setting.boolSetting(
        "plugins.flow_framework.multi_tenancy_enabled",
        false,
        Setting.Property.NodeScope
    );

    /** This setting sets max retryable tasks during workflow execution that polls results */
    public static final Setting<Integer> WORKFLOW_THREAD_POOL_SIZE = Setting.intSetting(
        "plugins.flow_framework.workflow_thread_pool_size",
        4,
        1,
        400,
        Setting.Property.NodeScope
    );

    /** This setting sets the max size of the provision thread pool */
    public static final Setting<Integer> PROVISION_THREAD_POOL_SIZE = Setting.intSetting(
        "plugins.flow_framework.provision_thread_pool_size",
        8,
        1,
        800,
        Setting.Property.NodeScope
    );

    /** This setting sets max workflows that can be simultaneously provisioned, or reprovisioned by the same tenant */
    public static final Setting<Integer> MAX_ACTIVE_PROVISIONS_PER_TENANT = Setting.intSetting(
        "plugins.flow_framework.max_active_provisions_per_tenant",
        2,
        1,
        40,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting sets the max size of the deprovision thread pool */
    public static final Setting<Integer> DEPROVISION_THREAD_POOL_SIZE = Setting.intSetting(
        "plugins.flow_framework.deprovision_thread_pool_size",
        4,
        1,
        400,
        Setting.Property.NodeScope
    );

    /** This setting sets max workflows that can be simultaneously deprovisioned by the same tenant */
    public static final Setting<Integer> MAX_ACTIVE_DEPROVISIONS_PER_TENANT = Setting.intSetting(
        "plugins.flow_framework.max_active_deprovisions_per_tenant",
        1,
        1,
        40,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    /** This setting sets the remote metadata type */
    public static final Setting<String> REMOTE_METADATA_TYPE = Setting.simpleString(
        "plugins.flow_framework." + REMOTE_METADATA_TYPE_KEY,
        Property.NodeScope,
        Property.Final
    );

    /** This setting sets the remote metadata endpoint */
    public static final Setting<String> REMOTE_METADATA_ENDPOINT = Setting.simpleString(
        "plugins.flow_framework." + REMOTE_METADATA_ENDPOINT_KEY,
        Property.NodeScope,
        Property.Final
    );

    /** This setting sets the remote metadata region */
    public static final Setting<String> REMOTE_METADATA_REGION = Setting.simpleString(
        "plugins.flow_framework." + REMOTE_METADATA_REGION_KEY,
        Property.NodeScope,
        Property.Final
    );

    /** This setting sets the remote metadata service name */
    public static final Setting<String> REMOTE_METADATA_SERVICE_NAME = Setting.simpleString(
        "plugins.flow_framework." + REMOTE_METADATA_SERVICE_NAME_KEY,
        Property.NodeScope,
        Property.Final
    );

    /**
     * Instantiate this class.
     *
     * @param clusterService OpenSearch cluster service
     * @param settings OpenSearch settings
     */
    public FlowFrameworkSettings(ClusterService clusterService, Settings settings) {
        // Currently this is just an on/off switch for the entire plugin's API.
        // If desired more fine-tuned feature settings can be added below.
        this.isFlowFrameworkEnabled = FLOW_FRAMEWORK_ENABLED.get(settings);
        this.retryDuration = TASK_REQUEST_RETRY_DURATION.get(settings);
        this.maxWorkflowSteps = MAX_WORKFLOW_STEPS.get(settings);
        this.maxWorkflows = MAX_WORKFLOWS.get(settings);
        this.requestTimeout = WORKFLOW_REQUEST_TIMEOUT.get(settings);
        this.isMultiTenancyEnabled = FLOW_FRAMEWORK_MULTI_TENANCY_ENABLED.get(settings);
        this.workflowThreadPoolSize = WORKFLOW_THREAD_POOL_SIZE.get(settings);
        this.provisionThreadPoolSize = PROVISION_THREAD_POOL_SIZE.get(settings);
        this.maxActiveProvisionsPerTenant = MAX_ACTIVE_PROVISIONS_PER_TENANT.get(settings);
        this.deprovisionThreadPoolSize = DEPROVISION_THREAD_POOL_SIZE.get(settings);
        this.maxActiveDeprovisionsPerTenant = MAX_ACTIVE_DEPROVISIONS_PER_TENANT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FLOW_FRAMEWORK_ENABLED, it -> isFlowFrameworkEnabled = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TASK_REQUEST_RETRY_DURATION, it -> retryDuration = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WORKFLOW_STEPS, it -> maxWorkflowSteps = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WORKFLOWS, it -> maxWorkflows = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(WORKFLOW_REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MAX_ACTIVE_PROVISIONS_PER_TENANT, it -> maxActiveProvisionsPerTenant = it);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MAX_ACTIVE_DEPROVISIONS_PER_TENANT, it -> maxActiveDeprovisionsPerTenant = it);
    }

    /**
     * Whether the flow framework feature is enabled. If disabled, no REST APIs will be available.
     * @return whether Flow Framework is enabled.
     */
    public boolean isFlowFrameworkEnabled() {
        return isFlowFrameworkEnabled;
    }

    /**
     * Getter for retry duration
     * @return retry duration
     */
    public TimeValue getRetryDuration() {
        return retryDuration;
    }

    /**
     * Getter for max workflow steps
     * @return count of steps
     */
    public Integer getMaxWorkflowSteps() {
        return maxWorkflowSteps;
    }

    /**
     * Getter for max workflows
     * @return max workflows
     */
    public Integer getMaxWorkflows() {
        return maxWorkflows;
    }

    /**
     * Getter for request timeout
     * @return request timeout
     */
    public TimeValue getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Whether multitenancy is enabled.
     * @return whether Flow Framework multitenancy is enabled.
     */
    public boolean isMultiTenancyEnabled() {
        return isMultiTenancyEnabled;
    }

    /**
     * Getter for workflow thread pool max size
     * @return workflow thread pool max
     */
    public Integer getWorkflowThreadPoolSize() {
        return workflowThreadPoolSize;
    }

    /**
     * Getter for provision thread pool max size
     * @return provision thread pool max
     */
    public Integer getProvisionThreadPoolSize() {
        return provisionThreadPoolSize;
    }

    /**
     * Getter for max active provisions per tenant
     * @return max active provisions
     */
    public Integer getMaxActiveProvisionsPerTenant() {
        return maxActiveProvisionsPerTenant;
    }

    /**
     * Getter for deprovision thread pool max size
     * @return deprovision thread pool max
     */
    public Integer getDeprovisionThreadPoolSize() {
        return deprovisionThreadPoolSize;
    }

    /**
     * Getter for max active deprovisions per tenant
     * @return max active deprovisions
     */
    public Integer getMaxActiveDeprovisionsPerTenant() {
        return maxActiveDeprovisionsPerTenant;
    }
}
