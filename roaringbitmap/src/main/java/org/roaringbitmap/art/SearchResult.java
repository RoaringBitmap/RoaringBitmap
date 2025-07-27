package org.roaringbitmap.art;

class SearchResult {

  static enum Outcome {
    FOUND,
    NOT_FOUND
  }

  final Outcome outcome;

  // Equivalent to <= key
  // Contains the exact position when FOUND or the next smaller one when NOT_FOUND
  private final int lessOrEqualPos;

  // Equivalent to > key
  // Only legal when outcome is NOT_FOUND
  private final int greaterPos;

  private SearchResult(Outcome outcome, int lessOrEqualPos, int greaterPos) {
    this.outcome = outcome;
    this.lessOrEqualPos = lessOrEqualPos;
    this.greaterPos = greaterPos;
  }

  static SearchResult found(int keyPos) {
    return new SearchResult(Outcome.FOUND, keyPos, BranchNode.ILLEGAL_IDX);
  }

  static SearchResult notFound(int lowerPos, int higherPos) {
    return new SearchResult(Outcome.NOT_FOUND, lowerPos, higherPos);
  }

  boolean hasKeyPos() {
    if (outcome == Outcome.FOUND) {
      // this would be an illegal state
      assert lessOrEqualPos != BranchNode.ILLEGAL_IDX;
      return true;
    }
    return false;
  }

  int getKeyPos() {
    if (outcome == Outcome.FOUND) {
      return lessOrEqualPos;
    }
    throw new IllegalAccessError("Only results with outcome FOUND have this field!");
  }

  boolean hasNextSmallerPos() {
    return outcome == Outcome.NOT_FOUND && lessOrEqualPos != BranchNode.ILLEGAL_IDX;
  }

  int getNextSmallerPos() {
    if (outcome == Outcome.NOT_FOUND) {
      return lessOrEqualPos;
    }
    throw new IllegalAccessError("Only results with outcome NOT_FOUND have this field!");
  }

  boolean hasNextLargerPos() {
    return outcome == Outcome.NOT_FOUND && greaterPos != BranchNode.ILLEGAL_IDX;
  }

  int getNextLargerPos() {
    if (outcome == Outcome.NOT_FOUND) {
      return greaterPos;
    }
    throw new IllegalAccessError("Only results with outcome NOT_FOUND have this field!");
  }
}
