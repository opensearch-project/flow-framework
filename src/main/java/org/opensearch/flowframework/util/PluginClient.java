/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.identity.Subject;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.FilterClient;

/**
 * A special client for executing transport actions as this plugin's system subject.
 */
public class PluginClient extends FilterClient {

    private static final Logger logger = LogManager.getLogger(PluginClient.class);

    private Subject subject;

    /**
     * Constructor for PluginClient
     * @param client the underlying client to wrap
     */
    public PluginClient(Client client) {
        super(client);
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        if (subject == null) {
            throw new IllegalStateException("PluginClient is not initialized.");
        }
        try (ThreadContext.StoredContext ctx = threadPool().getThreadContext().stashContext()) {
            subject.runAs(() -> {
                logger.info("Running transport action with subject: {}", subject.getPrincipal().getName());
                super.doExecute(action, request, ActionListener.runBefore(listener, ctx::restore));
            });
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }
}
