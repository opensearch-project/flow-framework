/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework;

import com.google.common.collect.ImmutableList;
import org.opensearch.action.ActionRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.flowframework.common.FlowFrameworkFeatureEnabledSetting;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.rest.RestCreateWorkflowAction;
import org.opensearch.flowframework.rest.RestProvisionWorkflowAction;
import org.opensearch.flowframework.rest.RestSearchWorkflowAction;
import org.opensearch.flowframework.transport.*;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.flowframework.common.CommonValue.FLOW_FRAMEWORK_THREAD_POOL_PREFIX;
import static org.opensearch.flowframework.common.CommonValue.PROVISION_THREAD_POOL;

/**
 * An OpenSearch plugin that enables builders to innovate AI apps on OpenSearch.
 */
public class FlowFrameworkPlugin extends Plugin implements ActionPlugin {

    private FlowFrameworkFeatureEnabledSetting flowFrameworkFeatureEnabledSetting;

    /**
     * Instantiate this plugin.
     */
    public FlowFrameworkPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        Settings settings = environment.settings();
        flowFrameworkFeatureEnabledSetting = new FlowFrameworkFeatureEnabledSetting(clusterService, settings);

        MachineLearningNodeClient mlClient = new MachineLearningNodeClient(client);
        WorkflowStepFactory workflowStepFactory = new WorkflowStepFactory(clusterService, client, mlClient);
        WorkflowProcessSorter workflowProcessSorter = new WorkflowProcessSorter(workflowStepFactory, threadPool);

        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler = new FlowFrameworkIndicesHandler(client, clusterService);

        return ImmutableList.of(workflowStepFactory, workflowProcessSorter, flowFrameworkIndicesHandler);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return ImmutableList.of(
            new RestCreateWorkflowAction(flowFrameworkFeatureEnabledSetting),
            new RestProvisionWorkflowAction(flowFrameworkFeatureEnabledSetting),
            new RestSearchWorkflowAction(flowFrameworkFeatureEnabledSetting)
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return ImmutableList.of(
            new ActionHandler<>(CreateWorkflowAction.INSTANCE, CreateWorkflowTransportAction.class),
            new ActionHandler<>(ProvisionWorkflowAction.INSTANCE, ProvisionWorkflowTransportAction.class),
            new ActionHandler<>(SearchWorkflowAction.INSTANCE, SearchWorkflowTransportAction.class)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = ImmutableList.of(FlowFrameworkFeatureEnabledSetting.FLOW_FRAMEWORK_ENABLED);
        return settings;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        // TODO : Determine final size/queueSize values for the provision thread pool
        return ImmutableList.of(
            new FixedExecutorBuilder(
                settings,
                PROVISION_THREAD_POOL,
                OpenSearchExecutors.allocatedProcessors(settings),
                10,
                FLOW_FRAMEWORK_THREAD_POOL_PREFIX + PROVISION_THREAD_POOL
            )
        );
    }

}
