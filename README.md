
[![Maven Test Build](https://github.com/jexxa-projects/ESPAdapters/actions/workflows/mavenBuild.yml/badge.svg)](https://github.com/jexxa-projects/ESPAdapters/actions/workflows/mavenBuild.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.jexxa/esp-adapters)](https://maven-badges.herokuapp.com/maven-central/io.jexxa/esp-adapters/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# ESPAdapters
This template can be used to start your own JLegMed application
 
##  Requirements

*   Java 17+ installed
*   IDE with maven support 

## Build the Project

*   Checkout the new project in your favorite IDE

*   [Optional] **With** running [developer stack](deploy/developerStack.yml):
    ```shell
    mvn clean install
    
    ```

Maven:
```xml
<dependencies>
    <dependency>
       <groupId>io.jexxa.esp</groupId>
       <artifactId>esp-adapters-core</artifactId>
       <version>0.4.0</version>
    </dependency>
    
</dependencies>
```

Gradle:

```groovy
compile "io.jexxa.esp:esp-adapters:0.4.0"
``` 

