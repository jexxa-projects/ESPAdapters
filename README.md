
[![Maven Test Build](https://github.com/jexxa-projects/ESPAdapters/actions/workflows/mavenBuild.yml/badge.svg)](https://github.com/jexxa-projects/ESPAdapters/actions/workflows/mavenBuild.yml)
[![New Release](https://github.com/jexxa-projects/ESPAdapters/actions/workflows/newRelease.yml/badge.svg)](https://github.com/jexxa-projects/ESPAdapters/actions/workflows/newRelease.yml)

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
       <version>0.3.0</version>
    </dependency>
    
</dependencies>
```

Gradle:

```groovy
compile "io.jexxa.esp:esp-adapters:0.3.0"
``` 

