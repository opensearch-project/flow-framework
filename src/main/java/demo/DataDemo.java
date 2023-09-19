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
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.flowframework.template.ProcessNode;
import org.opensearch.flowframework.template.TemplateParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Demo class exercising {@link TemplateParser}. This will be moved to a unit test.
 */
public class DataDemo {

    private static final Logger logger = LogManager.getLogger(DataDemo.class);

    /**
     * Demonstrate parsing a JSON graph.
     *
     * @param args unused
     */
    @SuppressForbidden(reason = "just a demo class that will be deleted")
    public static void main(String[] args) {
        String path = "src/test/resources/template/datademo.json";
        String json;
        try {
            json = new String(Files.readAllBytes(PathUtils.get(path)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Failed to read JSON at path {}", path);
            return;
        }

        logger.info("Parsing graph to sequence...");
        List<ProcessNode> processSequence = TemplateParser.parseJsonGraphToSequence(json);
        List<CompletableFuture<?>> futureList = new ArrayList<>();

        for (ProcessNode n : processSequence) {
            Set<ProcessNode> predecessors = n.getPredecessors();
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
    }

}
