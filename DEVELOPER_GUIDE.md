- [Developer Guide](#developer-guide)
    - [Forking and Cloning](#forking-and-cloning)
    - [Install Prerequisites](#install-prerequisites)
        - [Java](#java)
    - [Setup](#setup)
    - [Build](#build)
        - [Building from the command line](#building-from-the-command-line)
        - [Building from the IDE](#building-from-the-ide)
    - [Backports](#backports)
    - [Publishing](#publishing)
        - [Publishing to Maven Local](#publishing-to-maven-local)
        - [Generating artifacts](#generating-artifacts)
    - [Adding Workflow Steps](#adding-workflow-steps)

## Developer Guide

### Forking and Cloning

Fork this repository on GitHub, and clone locally with `git clone`.

### Install Prerequisites

See [OpenSearch requirements](https://github.com/opensearch-project/OpenSearch/blob/main/DEVELOPER_GUIDE.md#install-prerequisites).

#### Java

The Flow Framework `main` branch targets JDK 21. To ease backporting to `2.x`, maintain compatibility with JDK 11 unless significant benefits can be gained. Other plugins may require newer Java versions if used.

### Setup

1. Clone the repository (see [Forking and Cloning](#forking-and-cloning))
2. Make sure `JAVA_HOME` is pointing to a Java 21 or higher JDK (see [Install Prerequisites](#install-prerequisites))
3. Launch Intellij IDEA, Choose Import Project and select the settings.gradle file in the root of this package.

### Build

This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build. we also use the OpenSearch build tools for Gradle. These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in this package will work the way it's described in the Gradle documentation. If you encounter such a situation, the OpenSearch build tools [source code](https://github.com/opensearch-project/OpenSearch/tree/main/buildSrc/src/main/groovy/org/opensearch/gradle) is your best bet for figuring out what's going on.

#### Building from the command line

1. `./gradlew check` builds and tests.
2. `./gradlew :run` installs and runs ML-Commons and Flow Framework Plugins into a local cluster
3. `./gradlew run -Dsecurity.enabled=true` installs, configures and runs ML-Commons, Flow Framework and Security Plugins into a local cluster
4. `./gradlew spotlessApply` formats code. And/or import formatting rules in [formatterConfig.xml](formatter/formatterConfig.xml) with IDE.
5. `./gradlew test` to run the complete test suite.
6. `./gradlew integTest` to run only the non-security enabled integration tests
7. `./gradlew integTest -Dsecurity.enabled=true` to run only the security enabled integration tests
6. `./gradlew integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=docker-cluster` to run only the non-security enabled integration tests on a remote cluster
7. `./gradlew integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername=docker-cluster -Dsecurity.enabled=true` to run only the security enabled integration tests on a remote cluster

#### Building from the IDE

Currently, the only IDE we support is IntelliJ IDEA.  It's free, it's open source, it works. The gradle tasks above can also be launched from IntelliJ's Gradle toolbar and the extra parameters can be passed in via the Launch Configurations VM arguments.

### Backports

The Github workflow in [`backport.yml`](.github/workflows/backport.yml) creates backport PRs automatically when the
original PR with an appropriate label `backport <backport-branch-name>` is merged to main with the backport workflow
run successfully on the PR. For example, if a PR on main needs to be backported to `2.x` branch, add a label
`backport 2.x` to the PR and make sure the backport workflow runs on the PR along with other checks. Once this PR is
merged to main, the workflow will create a backport PR to the `2.x` branch.

### Publishing

#### Publishing to Maven Local

Run the below command to publish the artifacts to maven local.
```./gradlew publishToMavenLocal```

#### Generating artifacts

To generate the below artifacts on local
```
snapshots/
└── org
    └── opensearch
        └── plugin
            └── opensearch-flow-framework
                ├── 3.0.0.0-SNAPSHOT
                │   ├── maven-metadata.xml
                │   ├── maven-metadata.xml.md5
                │   ├── maven-metadata.xml.sha1
                │   ├── maven-metadata.xml.sha256
                │   ├── maven-metadata.xml.sha512
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.pom
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.pom.md5
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.pom.sha1
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.pom.sha256
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.pom.sha512
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.zip
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.zip.md5
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.zip.sha1
                │   ├── opensearch-flow-framework-3.0.0.0-20231005.170838-1.zip.sha256
                │   └── opensearch-flow-framework-3.0.0.0-20231005.170838-1.zip.sha512
                ├── maven-metadata.xml
                ├── maven-metadata.xml.md5
                ├── maven-metadata.xml.sha1
                ├── maven-metadata.xml.sha256
                └── maven-metadata.xml.sha512
```

1. Change the url from ``"https://ci.opensearch.org/ci/dbc/snapshots/maven/"`` to your local path and comment out the credentials under publishing/repositories in build.gradle.
2. Run ```./gradlew publishPluginZipPublicationToSnapshotsRepository```.

### Adding Workflow Steps

To add functionality to workflows, add new Workflow Steps to the [`org.opensearch.flowframework.workflow`](https://github.com/opensearch-project/flow-framework/tree/main/src/main/java/org/opensearch/flowframework/workflow) package.
1. Implement the [WorkflowStep](https://github.com/opensearch-project/flow-framework/blob/main/src/main/java/org/opensearch/flowframework/workflow/WorkflowStep.java) interface. See existing steps for examples for input, output, and API execution.
2. Choose a unique name for the step which is not used by other steps. This will align with the `step_type` field in the templates and should be descriptive of what the step does.
3. Add a constructor and call it from the [WorkflowStepFactory](https://github.com/opensearch-project/flow-framework/blob/main/src/main/java/org/opensearch/flowframework/workflow/WorkflowStepFactory.java).
4. Add an entry to the [WorkflowStepFactory](https://github.com/opensearch-project/flow-framework/blob/main/src/main/java/org/opensearch/flowframework/workflow/WorkflowStepFactory.java) enum specifying required inputs, outputs, required plugins, and optionally a different timeout than the default.
5. If your step provisions a resource that should be deprovisioned, create the corresponding step and add both steps to the [`WorkflowResources`](https://github.com/opensearch-project/flow-framework/blob/main/src/main/java/org/opensearch/flowframework/common/WorkflowResources.java) enum.
6. Write unit and integration tests.
