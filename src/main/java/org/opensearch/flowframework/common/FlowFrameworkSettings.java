/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.common;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

/** The common settings of flow framework  */
public class FlowFrameworkSettings {

    private FlowFrameworkSettings() {}

    /** The upper limit of max workflows that can be created  */
    public static final int MAX_WORKFLOWS_LIMIT = 10000;

    /** This setting sets max workflows that can be created  */
    public static final Setting<Integer> MAX_WORKFLOWS = Setting.intSetting(
        "plugins.flow_framework.max_workflows",
        1000,
        0,
        MAX_WORKFLOWS_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /** This setting sets the timeout for the request  */
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

    /** This setting sets the maximum number of get task request retries */
    public static final Setting<Integer> MAX_GET_TASK_REQUEST_RETRY = Setting.intSetting(
        "plugins.flow_framework.max_get_task_request_retry",
        5,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
