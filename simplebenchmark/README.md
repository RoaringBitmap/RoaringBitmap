# Simple benchmark

Java tooling is often excellent. JMH is a great framework for benchmarking Java software.
Sadly, it comes at a price: abstraction. This abstraction tends to make it difficult
to understand what is going on and, more critically, it can lead to serious performance
issues due to rising complexity.

To assess the performance effects of a code change should not take hours. It should take
minutes. It should not require complex commands and lots of wizardry. 

In effect, the tools need to empower the programmer, not weight him down.

This little program is an attempt to do this. In under a minute, we get usable benchmark
numbers... Under Linux or macOS type:

```bash
mvn -f ../pom.xml package -DskipTests -Dgpg.skip=true
./run.sh
```
