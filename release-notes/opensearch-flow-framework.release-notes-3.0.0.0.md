## Version 3.0.0.0 Release Notes

Compatible with OpenSearch 3.0.0

### Features
- Add per-tenant provisioning throttling ([#1074](https://github.com/opensearch-project/flow-framework/pull/1074))

### Bug Fixes
- Change REST status codes for RBAC and provisioning ([#1083](https://github.com/opensearch-project/flow-framework/pull/1083))
- Fix Config parser does not handle tenant_id field ([#1096](https://github.com/opensearch-project/flow-framework/pull/1096))
- Complete action listener on failed synchronous workflow provisioning ([#1098](https://github.com/opensearch-project/opensearch-remote-metadata-sdk/pull/1098))
- Add new attributes field to ToolStep ([#1113](https://github.com/opensearch-project/flow-framework/pull/1113))
- Fix bug handleReprovision missing wait_for_completion_timeout response ([#1107](https://github.com/opensearch-project/flow-framework/pull/1107))

### Maintenance
- Fix breaking changes for 3.0.0 release ([#1026](https://github.com/opensearch-project/flow-framework/pull/1026))
- Migrate from BC to BCFIPS libraries ([#1087](https://github.com/opensearch-project/flow-framework/pull/1087))

### Infrastructure
- Set Java target compatibility to JDK 21 ([#730](https://github.com/opensearch-project/flow-framework/pull/730))
- Use java-agent Gradle plugin to support phasing off SecurityManager usage in favor of Java Agent ([#1108](https://github.com/opensearch-project/flow-framework/pull/1108))


### Documentation
- Add text to visualization agent template ([#936](https://github.com/opensearch-project/flow-framework/pull/936))
