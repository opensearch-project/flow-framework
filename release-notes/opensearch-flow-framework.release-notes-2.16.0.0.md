## Version 2.16.0.0

Compatible with OpenSearch 2.16.0

### Enhancements
- Register system index descriptors through SystemIndexPlugin.getSystemIndexDescriptors ([#750](https://github.com/opensearch-project/flow-framework/pull/750))
- Support editing of certain workflow fields on a provisioned workflow ([#757](https://github.com/opensearch-project/flow-framework/pull/757))
- Add allow_delete parameter to Deprovision API ([#763](https://github.com/opensearch-project/flow-framework/pull/763))
- Improve Template and WorkflowState builders ([#778](https://github.com/opensearch-project/flow-framework/pull/778))

### Infrastructure
- Update dependency com.fasterxml.jackson.core:jackson-core to v2.17.2 ([#760](https://github.com/opensearch-project/flow-framework/pull/760))
- Update dependency gradle to v8.8 ([#725](https://github.com/opensearch-project/flow-framework/pull/725))
- Update dependency org.junit.jupiter:junit-jupiter to v5.10.3 ([#752](https://github.com/opensearch-project/flow-framework/pull/752))
- Update dependency org.apache.httpcomponents.core5:httpcore5 to v5.2.5 ([#754](https://github.com/opensearch-project/flow-framework/pull/754))
- Update plugin io.github.surpsg.delta-coverage to v2.3.0 ([#772](https://github.com/opensearch-project/flow-framework/pull/772))
- Update dependency gradle to v8.9 ([#777](https://github.com/opensearch-project/flow-framework/pull/777))
- Update dependency org.eclipse.platform:org.eclipse.core.runtime to v3.31.100 ([#737](https://github.com/opensearch-project/flow-framework/pull/737))
- Update plugin org.gradle.test-retry to v1.5.10 ([#784](https://github.com/opensearch-project/flow-framework/pull/784))
- Update dependency org.apache.commons:commons-lang3 to v3.15.0 ([#791](https://github.com/opensearch-project/flow-framework/pull/791))

### Maintenance
- Change deprecated Gradle wrapper validation workflow to replacement ([#764](https://github.com/opensearch-project/flow-framework/pull/764))
- Allow lower NodeJS version on GH runners for security test ([#765](https://github.com/opensearch-project/flow-framework/pull/765))
- Switch diffCoverage plugin to deltaCoverage ([#762](https://github.com/opensearch-project/flow-framework/pull/762))
- Update PULL_REQUEST_TEMPLATE to include an API spec change in the checklist ([#773](https://github.com/opensearch-project/flow-framework/pull/773))
- Stop duplicate CI runs on branch push by bots ([#755](https://github.com/opensearch-project/flow-framework/pull/755))
- Switch to arm64 runners on macOS ([#794](https://github.com/opensearch-project/flow-framework/pull/794))



