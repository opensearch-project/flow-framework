- [Developer Guide](#developer-guide)
    - [Forking and Cloning](#forking-and-cloning)
    - [Install Prerequisites](#install-prerequisites)
        - [JDK 14](#jdk-14)
    - [Setup](#setup)
    - [Build](#build)
        - [Building from the command line](#building-from-the-command-line)
        - [Building from the IDE](#building-from-the-ide)
    - [Backports](#backports)
    - [Publishing](#publishing)
        - [Publishing to Maven Local](#publishing-to-maven-local)
        - [Generating artifacts](#generating-artifacts)

## Developer Guide

### Forking and Cloning

Fork this repository on GitHub, and clone locally with `git clone`.

### Install Prerequisites

#### JDK 14

OpenSearch components build using Java 14 at a minimum. This means you must have a JDK 14 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 14 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-14`.

### Setup

1. Clone the repository (see [Forking and Cloning](#forking-and-cloning))
2. Make sure `JAVA_HOME` is pointing to a Java 14 JDK (see [Install Prerequisites](#install-prerequisites))
3. Launch Intellij IDEA, Choose Import Project and select the settings.gradle file in the root of this package.

### Build

This package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build. we also use the OpenSearch build tools for Gradle. These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in this package will work the way it's described in the Gradle documentation. If you encounter such a situation, the OpenSearch build tools [source code](https://github.com/opensearch-project/OpenSearch/tree/main/buildSrc/src/main/groovy/org/opensearch/gradle) is your best bet for figuring out what's going on.

#### Building from the command line

1. `./gradlew check` builds and tests.
2. `./gradlew :run` runs the plugin.
3. `./gradlew spotlessApply` formats code. And/or import formatting rules in [formatterConfig.xml](formatter/formatterConfig.xml) with IDE.
4. `./gradlew test` to run the complete test suite.

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
            └── opensearch-ai-flow-framework
                ├── 3.0.0.0-SNAPSHOT
                │   ├── maven-metadata.xml
                │   ├── maven-metadata.xml.md5
                │   ├── maven-metadata.xml.sha1
                │   ├── maven-metadata.xml.sha256
                │   ├── maven-metadata.xml.sha512
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.pom
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.pom.md5
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.pom.sha1
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.pom.sha256
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.pom.sha512
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.zip
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.zip.md5
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.zip.sha1
                │   ├── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.zip.sha256
                │   └── opensearch-ai-flow-framework-3.0.0.0-20231005.170838-1.zip.sha512
                ├── maven-metadata.xml
                ├── maven-metadata.xml.md5
                ├── maven-metadata.xml.sha1
                ├── maven-metadata.xml.sha256
                └── maven-metadata.xml.sha512


```
1. Change the url from ``"https://aws.oss.sonatype.org/content/repositories/snapshots"`` to your local path and comment out the credentials under publishing/repositories in build.gradle.
2. Run ```./gradlew publishPluginZipPublicationToSnapshotsRepository```.
