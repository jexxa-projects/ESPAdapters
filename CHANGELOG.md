# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## \[2.0.0] - 2026-05-14
### Changed
- **Breaking Change (Coordinates)**: Unified Maven GroupIDs and ArtifactIDs to improve SBOM data quality and supply chain clarity.
    - Parent: `io.jexxa:esp` -> `io.jexxa:adapters`
    - Modules now inherit the base GroupID `io.jexxa`.
- **Compliance**: Updated SBOM generation to **CycloneDX Schema 1.6** for BSI TR-03183-2 compliance.

### Fix
- *(java)* Bump io.jexxa.adapters:common-adapters to 4.1.0)
- *(java)* Bump org.slf4j:slf4j-simple from 2.0.17 to 2.0.18 - ([91f1bdb](https://github.com/jexxa-projects/ESPAdapters/commit/91f1bdbbcaae865286abe60111287915d3f1a322))

## \[1.0.3] - 2026-04-24

### Fix
- updated dependencies
- 
## \[1.0.2] - 2026-04-04

### Fix
- updated dependencies
- *(java)* Bump io.jexxa.common:common-adapters from 3.0.3 to 3.0.4 - ([3092571](https://github.com/jexxa-projects/ESPAdapters/commit/3092571ea1ba2aff4d3715b85987ecfbdd48ce38))


## \[1.0.1] - 2026-03-07

### Fix
- updated dependencies

## \[1.0.0] - 2026-02-07
### Added   
- first major release

### Fix
- updated dependencies

## \[0.7.1] - 2026-01-06
### Fix
- Retry mechanism in Kafka -> If an exception during processing occurs, we do not commit the message and seek back to the last commited position     

## \[0.7.0] - 2025-12-16
### Added
- Method to configure topic properties when creating a new topic 

## \[0.6.0] - 2025-11-08
### Changed
- esp-adapters: Major refactoring to jexxa-commons 3.0.0
- Support for java25+ only 

- esp-adapters-test: DigiSpine is now called EventStreamingPlatform

## \[0.5.0] - 2025-09-07
### Added
- ESPProducer no longer calls flush after each `send` call. Flush is called when cleaning up open connections 

## \[0.4.0] - 2025-08-21
### Added
- Possibility to define headers 

## \[0.3.0] - 2025-08-05
### Changed
- Extended KafkaPool messages to set retention time and create topics for bootstrapping

## \[0.2.0] - 2025-07-17
### Changed
- ESP-ADAPTERS-TEST: Added test methods for DigiSpine to read messages from Kafka

