/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.transport.client.Client;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.CONFIGURATIONS;
import static org.opensearch.flowframework.common.WorkflowResources.INDEX_NAME;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to update index settings and mappings, currently only update settings is implemented
 */
public class UpdateIndexStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(UpdateIndexStep.class);
    private final Client client;

    /** The name of this step */
    public static final String NAME = "update_index";

    /**
     * Instantiate this class
     *
     * @param client Client to update an index
     */
    public UpdateIndexStep(Client client) {
        this.client = client;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params,
        String tenantId
    ) {
        PlainActionFuture<WorkflowData> updateIndexFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(INDEX_NAME, CONFIGURATIONS);
        Set<String> optionalKeys = Collections.emptySet();

        try {

            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String indexName = (String) inputs.get(INDEX_NAME);
            String configurations = (String) inputs.get(CONFIGURATIONS);
            byte[] byteArr = configurations.getBytes(StandardCharsets.UTF_8);
            BytesReference configurationsBytes = new BytesArray(byteArr);

            UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);

            if (configurations.isEmpty()) {
                String errorMessage = "Failed to update index settings for index " + indexName + ", index configuration is not given";
                throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
            } else {

                Map<String, Object> sourceAsMap = XContentHelper.convertToMap(configurationsBytes, false, MediaTypeRegistry.JSON).v2();

                // TODO : Add support to update index mappings

                // extract index settings from configuration
                if (!sourceAsMap.containsKey("settings")) {
                    String errorMessage = "Failed to update index settings for index "
                        + indexName
                        + ", settings are not found in the index configuration";
                    throw new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST);
                } else {

                    @SuppressWarnings("unchecked")
                    Map<String, Object> updatedSettings = (Map<String, Object>) sourceAsMap.get("settings");

                    // check if settings are flattened or expanded
                    Map<String, Object> flattenedSettings = new HashMap<>();
                    if (updatedSettings.containsKey("index")) {
                        ParseUtils.flattenSettings("", updatedSettings, flattenedSettings);
                    } else {
                        // Create index setting configuration can be a mix of flattened or expanded settings
                        // prepend index. to ensure successful setting comparison

                        flattenedSettings.putAll(ParseUtils.prependIndexToSettings(updatedSettings));
                    }

                    Map<String, Object> filteredSettings = new HashMap<>();

                    // Retrieve current Index Settings
                    GetSettingsRequest getSettingsRequest = new GetSettingsRequest();
                    getSettingsRequest.indices(indexName);
                    getSettingsRequest.includeDefaults(true);
                    client.admin().indices().getSettings(getSettingsRequest, ActionListener.wrap(response -> {
                        Map<String, Settings> indexToSettings = new HashMap<String, Settings>(response.getIndexToSettings());

                        // Include in the update request only settings with updated values
                        Settings currentIndexSettings = indexToSettings.get(indexName);
                        for (Map.Entry<String, Object> e : flattenedSettings.entrySet()) {
                            String val = e.getValue().toString();
                            if (!val.equals(currentIndexSettings.get(e.getKey()))) {
                                filteredSettings.put(e.getKey(), e.getValue());
                            }
                        }

                        // Create and send the update settings request
                        updateSettingsRequest.settings(filteredSettings);
                        if (updateSettingsRequest.settings().size() == 0) {
                            String errorMessage = "Failed to update index settings for index "
                                + indexName
                                + ", no settings have been updated";
                            updateIndexFuture.onFailure(new WorkflowStepException(errorMessage, RestStatus.BAD_REQUEST));
                        } else {
                            client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(acknowledgedResponse -> {
                                String resourceName = getResourceByWorkflowStep(getName());
                                logger.info("Updated index settings for index {}", indexName);
                                updateIndexFuture.onResponse(
                                    new WorkflowData(Map.of(resourceName, indexName), currentNodeInputs.getWorkflowId(), currentNodeId)
                                );

                            }, ex -> {
                                Exception e = getSafeException(ex);
                                String errorMessage = (e == null
                                    ? ParameterizedMessageFactory.INSTANCE.newMessage(
                                        "Failed to update the index settings for index {}",
                                        indexName
                                    ).getFormattedMessage()
                                    : e.getMessage());
                                logger.error(errorMessage, e);
                                updateIndexFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                            }));
                        }
                    }, ex -> {
                        Exception e = getSafeException(ex);
                        String errorMessage = (e == null
                            ? ParameterizedMessageFactory.INSTANCE.newMessage(
                                "Failed to retrieve the index settings for index {}",
                                indexName
                            ).getFormattedMessage()
                            : e.getMessage());
                        logger.error(errorMessage, e);
                        updateIndexFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                    }));
                }
            }
        } catch (Exception e) {
            updateIndexFuture.onFailure(new WorkflowStepException(e.getMessage(), ExceptionsHelper.status(e)));
        }

        return updateIndexFuture;

    }

    @Override
    public String getName() {
        return NAME;
    }

}
