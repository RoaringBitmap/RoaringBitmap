package org.roaringbitmap.insights;

/**
 * The purpose of this class it to help user decide
 * which {@link org.roaringbitmap.RoaringBitmapWriter} heuristic to use.
 */
public class NaiveWriterRecommender {
  /**
   * Based on the statistics it applies expert rules
   * to help tuning the {@link org.roaringbitmap.RoaringBitmapWriter}
   * @param s statistics
   * @return some message
   */
  public static String recommend(BitmapStatistics s) {
    if (s.containerCount() == 0) {
      return "Empty statistics, cannot recommend.";
    }
    StringBuilder sb = new StringBuilder();
    containerCountRecommendations(s, sb);
    double acFraction = s.containerFraction(s.getArrayContainersStats().getContainersCount());
    if (acFraction > ArrayContainersDomination) {
      if (s.getArrayContainersStats().averageCardinality() < WorthUsingArraysCardinalityThreshold) {
        arrayContainerRecommendations(s, sb);
      } else {
        denseArrayWarning(sb);
        constantMemoryRecommendation(s, sb);
      }
    } else if (s.containerFraction(s.getRunContainerCount()) > RunContainersDomination) {
      runContainerRecommendations(sb);
    } else {
      constantMemoryRecommendation(s, sb);
    }
    return sb.toString();
  }

  private static void denseArrayWarning(StringBuilder sb) {
    sb
      .append("Most of your containers are array containers, ")
      .append("but with quite significant cardinality.\n")
      .append("It should be better to start with .constantMemory() ")
      .append("that can scale down to ArrayContainer anyway.");
  }

  private static void runContainerRecommendations(StringBuilder sb) {
    sb
      .append(".optimiseForRuns(), because over ")
      .append(RunContainersDomination)
      .append(" containers are of type RunContainer.\n")
      .append("Make sure to try .constantMemory()")
      .append("as inserting to RunContainers might not be that efficient.");
  }

  private static void constantMemoryRecommendation(BitmapStatistics s, StringBuilder sb) {
    long buffersSizeBytes = s.getBitmapsCount() * Long.BYTES * 1024L;
    long bufferSizeMiB = buffersSizeBytes / (1024 * 1024);
    sb
      .append(".constantMemory() is sensible default for most use cases.\n")
      .append("Be prepared to allocate on heap ")
      .append(bufferSizeMiB)
      .append(" [MiB] just for buffers if you have them open at the same time.");
  }

  private static void arrayContainerRecommendations(BitmapStatistics s, StringBuilder sb) {
    double acFraction = s.containerFraction(s.getArrayContainersStats().getContainersCount());
    sb.append(".optimiseForArrays(), because fraction of ArrayContainers ")
      .append(acFraction)
      .append(" is over arbitrary threshold ")
      .append(ArrayContainersDomination)
      .append("\n")
      .append(".expectedContainerSize(")
      .append(s.getArrayContainersStats().averageCardinality())
      .append(") to preallocate array containers for average number of elements.\n");
  }

  private static void containerCountRecommendations(BitmapStatistics basedOn, StringBuilder sb) {
    long averageContainersCount = basedOn.containerCount() / basedOn.getBitmapsCount();
    sb.append(".initialCapacity(")
      .append(averageContainersCount)
      .append("), because on average each bitmap has ")
      .append(averageContainersCount)
      .append(" containers.\n");
  }

  private static double ArrayContainersDomination = 0.75;
  private static int WorthUsingArraysCardinalityThreshold = 4096 / 2;
  private static double RunContainersDomination = 0.8;

}
