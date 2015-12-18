@echo off 
start javaw -jar -Xms256M -Xmx720M -XX:+UseParallelGC -XX:MinHeapFreeRatio=40 -XX:ParallelGCThreads=2 .\antlrworks-1.4.jar 
exit 
