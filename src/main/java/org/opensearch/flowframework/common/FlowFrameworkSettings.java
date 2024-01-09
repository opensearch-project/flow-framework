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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

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
        false,
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
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FLOW_FRAMEWORK_ENABLED, it -> isFlowFrameworkEnabled = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TASK_REQUEST_RETRY_DURATION, it -> retryDuration = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WORKFLOW_STEPS, it -> maxWorkflowSteps = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_WORKFLOWS, it -> maxWorkflows = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(WORKFLOW_REQUEST_TIMEOUT, it -> requestTimeout = it);
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
}
