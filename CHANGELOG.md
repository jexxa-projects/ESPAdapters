# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

