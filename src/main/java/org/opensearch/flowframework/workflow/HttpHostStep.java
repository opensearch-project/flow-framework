/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.ParseUtils;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.HOSTNAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.HTTP_HOST_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PORT_FIELD;
import static org.opensearch.flowframework.common.CommonValue.SCHEME_FIELD;

/**
 * Step to register parameters for an HTTP Connection to a Host
 */
public class HttpHostStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(HttpHostStep.class);
    PlainActionFuture<WorkflowData> hostFuture = PlainActionFuture.newFuture();
    static final String NAME = "http_host";

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        Set<String> requiredKeys = Set.of(SCHEME_FIELD, HOSTNAME_FIELD, PORT_FIELD);
        // TODO Possibly add credentials fields here
        // See ML Commons MLConnectorInput class and its usage
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

            String scheme = validScheme(inputs.get(SCHEME_FIELD));
            String hostname = validHostName(inputs.get(HOSTNAME_FIELD));
            int port = validPort(inputs.get(PORT_FIELD));

            HttpHost httpHost = new HttpHost(hostname, port, scheme);

            hostFuture.onResponse(
                new WorkflowData(
                    Map.ofEntries(Map.entry(HTTP_HOST_FIELD, httpHost)),
                    currentNodeInputs.getWorkflowId(),
                    currentNodeInputs.getNodeId()
                )
            );

            logger.info("Http Host registered successfully {}", httpHost);

        } catch (FlowFrameworkException e) {
            hostFuture.onFailure(e);
        }
        return hostFuture;
    }

    private String validScheme(Object o) {
        String scheme = o.toString().toLowerCase(Locale.ROOT);
        if ("http".equals(scheme) || "https".equals(scheme)) {
            return scheme;
        }
        throw new FlowFrameworkException("http_host scheme must be http or https", RestStatus.BAD_REQUEST);
    }

    private String validHostName(Object o) {
        // TODO Add validation:
        // Prevent use of localhost or private IP address ranges
        // See ML Commons MLHttpClientFactory.java methods for examples
        // Possibly consider an allowlist of addresses
        return o.toString();
    }

    private int validPort(Object o) {
        try {
            int port = Integer.parseInt(o.toString());
            if ((port & 0xffff0000) != 0) {
                throw new FlowFrameworkException("http_host port number must be between 0 and 65535", RestStatus.BAD_REQUEST);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new FlowFrameworkException("http_host port must be a number between 0 and 65535", RestStatus.BAD_REQUEST);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}
