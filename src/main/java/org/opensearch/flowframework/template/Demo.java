/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.template;

import org.opensearch.flowframework.workflow.Workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Demo class exercising {@link TemplateParser}. This will be moved to a unit test.
 */
public class Demo {

    private static Map<String, Workflow> workflowMap = new HashMap<>();
    static {
        workflowMap.put("fetch_model", new DemoWorkflowStep(3000));
        workflowMap.put("create_ingest_pipeline", new DemoWorkflowStep(4000));
        workflowMap.put("create_search_pipeline", new DemoWorkflowStep(8000));
        workflowMap.put("create_neural_search_index", new DemoWorkflowStep(2000));
    }

    /**
     * Demonstrate parsing a JSON graph.
     *
     * @param args unused
     */
    public static void main(String[] args) {
        String json = "{\n"
            + "    \"sequence\": {\n"
            + "        \"nodes\": [\n"
            + "            {\n"
            + "                \"id\": \"fetch_model\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"id\": \"create_ingest_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"id\": \"create_search_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"id\": \"create_neural_search_index\"\n"
            + "            }\n"
            + "        ],\n"
            + "        \"edges\": [\n"
            + "            {\n"
            + "                \"source\": \"fetch_model\",\n"
            + "                \"dest\": \"create_ingest_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"source\": \"fetch_model\",\n"
            + "                \"dest\": \"create_search_pipeline\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"source\": \"create_ingest_pipeline\",\n"
            + "                \"dest\": \"create_neural_search_index\"\n"
            + "            },\n"
            + "            {\n"
            + "                \"source\": \"create_search_pipeline\",\n"
            + "                \"dest\": \"create_neural_search_index\"\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}";

        System.out.println(json);

        System.out.println("Parsing graph to sequence...");
        List<ProcessNode> processSequence = TemplateParser.parseJsonGraphToSequence(json, workflowMap);
        List<CompletableFuture<?>> futureList = new ArrayList<>();

        for (ProcessNode n : processSequence) {
            Set<ProcessNode> predecessors = n.getPredecessors();
            System.out.format(
                "Queueing process [%s].  %s.%n",
                n.id(),
                predecessors.isEmpty()
                    ? "Can start immediately!"
                    : String.format(
                        "Must wait for [%s] to complete first.",
                        predecessors.stream().map(p -> p.id()).collect(Collectors.joining(", "))
                    )
            );
            futureList.add(n.execute());
        }
        futureList.forEach(CompletableFuture::join);
        System.out.println("All done!");
    }

}
