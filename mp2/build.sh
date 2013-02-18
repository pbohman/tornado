#!/bin/bash

mkdir -p classes
mkdir -p classes/WordCount
mkdir -p classes/IndexBuilder
mkdir -p classes/GalaxyAverage

javac -classpath hadoop-0.19.1-core.jar src/main/java/com/cs498/team17/mp2/WordCount/WordCount.java -d classes/WordCount
javac -classpath hadoop-0.19.1-core.jar src/main/java/com/cs498/team17/mp2/IndexBuilder/IndexBuilder.java -d classes/IndexBuilder
javac -classpath hadoop-0.19.1-core.jar src/main/java/com/cs498/team17/mp2/GalaxyAverage/GalaxyAverage.java -d classes/GalaxyAverage

cd classes/WordCount; jar -cvf ../../WordCount.jar .; cd ../..
cd classes/IndexBuilder; jar -cvf ../../IndexBuilder.jar .; cd ../..
cd classes/GalaxyAverage; jar -cvf ../../GalaxyAverage.jar .; cd ../..



