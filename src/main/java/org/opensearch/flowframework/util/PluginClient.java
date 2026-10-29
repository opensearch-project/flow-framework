/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.util;

import org.opensearch.transport.client.Client;

/**
 * Flow Framework's client that runs transport actions as this plugin's system subject.
 */
public final class PluginClient extends org.opensearch.commons.client.PluginClient {

    /**
     * Wraps a client so its actions run as the plugin subject, which is assigned later.
     *
     * @param client the client whose actions this wraps
     */
    public PluginClient(Client client) {
        super(client);
    }
}
