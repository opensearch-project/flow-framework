# CHANGELOG
All notable changes to this project are documented in this file.

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased 3.0](https://github.com/opensearch-project/flow-framework/compare/2.x...HEAD)
### Features
### Enhancements
### Bug Fixes
### Infrastructure
### Documentation
### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/flow-framework/compare/2.13...2.x)
### Features
### Enhancements
- Add guardrails to default use case params ([#658](https://github.com/opensearch-project/flow-framework/pull/658))
- Allow strings for boolean workflow step parameters ([#671](https://github.com/opensearch-project/flow-framework/pull/671))
- Add optional delay parameter to no-op step ([#674](https://github.com/opensearch-project/flow-framework/pull/674))

### Bug Fixes
- Reset workflow state to initial state after successful deprovision ([#635](https://github.com/opensearch-project/flow-framework/pull/635))
- Silently ignore content on APIs that don't require it ([#639](https://github.com/opensearch-project/flow-framework/pull/639))
- Hide user and credential field from search response ([#680](https://github.com/opensearch-project/flow-framework/pull/680))
- Throw the correct error message in status API for WorkflowSteps ([#676](https://github.com/opensearch-project/flow-framework/pull/676))
- Delete workflow state when template is deleted and no resources exist ([#689](https://github.com/opensearch-project/flow-framework/pull/689))

### Infrastructure
- Switch macos runner to macos-13 from macos-latest since macos-latest is now arm64 ([#686](https://github.com/opensearch-project/flow-framework/pull/686))

### Documentation
### Maintenance
### Refactoring
- Improve error messages for workflow states other than NOT_STARTED ([#642](https://github.com/opensearch-project/flow-framework/pull/642))
