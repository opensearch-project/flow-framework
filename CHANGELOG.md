# CHANGELOG
All notable changes to this project are documented in this file.

Inspired from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)

## [Unreleased 3.0](https://github.com/opensearch-project/flow-framework/compare/2.x...HEAD)
### Features
### Enhancements
### Bug Fixes
### Infrastructure
- Set Java target compatibility to JDK 21 ([#730](https://github.com/opensearch-project/flow-framework/pull/730))

### Documentation
- Add alert summary agent template ([#873](https://github.com/opensearch-project/flow-framework/pull/873))
- Add alert summary with log pattern agent template ([#945](https://github.com/opensearch-project/flow-framework/pull/945))
- Add text to visualization agent template ([#936](https://github.com/opensearch-project/flow-framework/pull/936))

### Maintenance
### Refactoring

## [Unreleased 2.x](https://github.com/opensearch-project/flow-framework/compare/2.17...2.x)
### Features
- Add synchronous execution option to workflow provisioning ([#990](https://github.com/opensearch-project/flow-framework/pull/990))
- Add ApiSpecFetcher for Fetching and Comparing API Specifications ([#651](https://github.com/opensearch-project/flow-framework/issues/651))
- Add optional config field to tool step ([#899](https://github.com/opensearch-project/flow-framework/pull/899))
- Add API Consistency Tests with ML-Common and Set Up Daily GitHub Action Trigger([#908](https://github.com/opensearch-project/flow-framework/issues/908))

### Enhancements
- Incrementally remove resources from workflow state during deprovisioning ([#898](https://github.com/opensearch-project/flow-framework/pull/898))

### Bug Fixes
- Remove useCase and defaultParams field in WorkflowRequest ([#758](https://github.com/opensearch-project/flow-framework/pull/758))
- Fixed Template Update Location and Improved Logger Statements in ReprovisionWorkflowTransportAction ([#918](https://github.com/opensearch-project/flow-framework/pull/918))
- Fix RBAC fetching from workflow state when template is not present ([#998](https://github.com/opensearch-project/flow-framework/pull/998))

### Infrastructure
### Documentation
- Add knowledge base alert agent into sample templates ([#874](https://github.com/opensearch-project/flow-framework/pull/874))
- Add query assist data summary agent into sample templates ([#875](https://github.com/opensearch-project/flow-framework/pull/875))
- Add suggest anomaly detector agent into sample templates ([#944](https://github.com/opensearch-project/flow-framework/pull/944))

### Maintenance
### Refactoring
- Update workflow state without using painless script ([#894](https://github.com/opensearch-project/flow-framework/pull/894))
- Replace String concatenation with Log4j ParameterizedMessage for readability ([#943](https://github.com/opensearch-project/flow-framework/pull/943))
