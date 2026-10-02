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

    // Assigned from IdentityAwarePlugin.assignSubject, which runs on a different thread than the
    // transport actions that read it.
    private volatile Subject subject;

    /**
     * Constructor for PluginClient
     * @param client the underlying client to wrap
     */
    public PluginClient(Client client) {
        super(client);
    }

    /**
     * Set the subject to use for executing transport actions.
     * @param subject the subject to use
     */
    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        Subject currentSubject = this.subject;
        if (currentSubject == null) {
            throw new IllegalStateException("PluginClient is not initialized with a subject.");
        }

        // Saves the caller's context, rather than clearing it, because runAs performs the subject
        // switch itself. Held in a local so the restore stays visibly tied to the listener, which is
        // where it has to happen: a transport response arrives on a pooled thread that never carried
        // the caller's context.
        ThreadContext.StoredContext storedContext = threadPool().getThreadContext().newStoredContext(false);

        try {
            currentSubject.runAs(() -> {
                logger.debug("Running transport action as subject: {}", currentSubject.getPrincipal().getName());
                super.doExecute(action, request, ActionListener.runBefore(listener, storedContext::restore));
            });
        } catch (Exception e) {
            // Reported through the listener rather than thrown, so an async caller is not left waiting.
            storedContext.close();
            listener.onFailure(e);
        }
    }
}
