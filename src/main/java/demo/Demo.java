/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package demo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.flowframework.model.Template;
import org.opensearch.flowframework.workflow.ProcessNode;
import org.opensearch.flowframework.workflow.WorkflowProcessSorter;
import org.opensearch.flowframework.workflow.WorkflowStepFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Demo class exercising {@link WorkflowProcessSorter}. This will be moved to a unit test.
 */
public class Demo {

    private static final Logger logger = LogManager.getLogger(Demo.class);

    /**
     * Demonstrate parsing a JSON graph.
     *
     * @param args unused
     * @throws IOException on a failure
     */
    @SuppressForbidden(reason = "just a demo class that will be deleted")
    public static void main(String[] args) throws IOException {
        String path = "src/test/resources/template/demo.json";
        String json;
        try {
            json = new String(Files.readAllBytes(PathUtils.get(path)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Failed to read JSON at path {}", path);
            return;
        }
        Client client = new NodeClient(null, null);
        WorkflowStepFactory factory = WorkflowStepFactory.create(client);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        WorkflowProcessSorter.create(factory, executor);

        logger.info("Parsing graph to sequence...");
        Template t = Template.parse(json);
        List<ProcessNode> processSequence = WorkflowProcessSorter.get().sortProcessNodes(t.workflows().get("demo"));
        List<CompletableFuture<?>> futureList = new ArrayList<>();

        for (ProcessNode n : processSequence) {
            List<ProcessNode> predecessors = n.predecessors();
            logger.info(
                "Queueing process [{}].{}",
                n.id(),
                predecessors.isEmpty()
                    ? " Can start immediately!"
                    : String.format(
                        Locale.getDefault(),
                        " Must wait for [%s] to complete first.",
                        predecessors.stream().map(p -> p.id()).collect(Collectors.joining(", "))
                    )
            );
            futureList.add(n.execute());
        }
        futureList.forEach(CompletableFuture::join);
        logger.info("All done!");
        executor.shutdown();
    }
}
