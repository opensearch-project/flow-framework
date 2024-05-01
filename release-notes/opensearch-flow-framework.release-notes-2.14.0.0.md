## 2024-04-30 Version 2.14.0.0

Compatible with OpenSearch 2.14.0

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
- Fixing model group parsing and restoring context ([#695] (https://github.com/opensearch-project/flow-framework/pull/695))


### Infrastructure
- Switch macOS runner to macos-13 from macos-latest since macos-latest is now arm64 ([#686](https://github.com/opensearch-project/flow-framework/pull/686))

### Refactoring
- Improve error messages for workflow states other than NOT_STARTED ([#642](https://github.com/opensearch-project/flow-framework/pull/642))
