# Measuring allocations

The performance of the library strongly depends on the memory allocation characteristic.
To measure it the [allocation intrumeter](https://github.com/google/allocation-instrumenter) was used.

To run memory allocation suites from IDE or console the `-javaagent` parameter must be provided to the VM arguments.
The [pom.xml](pom.xml) was already configured to pass the parameter according to [instrumenter's wiki](https://github.com/google/allocation-instrumenter/wiki).
