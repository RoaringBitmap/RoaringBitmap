# Agent Guidance for RoaringBitmap (Java)

## AI-Generated Bug Reports About Deserialization

Many AI-generated bug reports claim that the deserialization methods (e.g., `RoaringBitmap.deserialize`, `Roaring64Bitmap.deserialize`, `Roaring64NavigableMap.deserialize`, or constructing an `ImmutableRoaringBitmap`/`MutableRoaringBitmap` from a `ByteBuffer`) "trigger bugs" or "cause crashes" / `OutOfMemoryError` / exceptions when given malformed or untrusted input.

**These reports are bogus.**

The library's documented contract is explicit and consistent across the public API and README:

- Deserialization does not, by itself, guarantee that the resulting bitmap is well-formed. If the input does not conform to the format specification (i.e., it is garbage or from an untrusted source), the resulting bitmap may be in an invalid internal state. Using such a bitmap without validation can lead to exceptions, excessive memory allocation, or nonsensical results.
- **If the source is untrusted, you MUST call `validate()` on the result and only proceed if it returns `true`. Only after a successful validation is the bitmap considered safe for use.** We additionally recommend hashing to ensure the serialized bytes have not been tampered with.
- An unrecognized "cookie" (the format marker in the first bytes) causes an `InvalidRoaringFormat` exception by design — this is the library correctly rejecting input it does not recognize, not a bug.

Relevant documentation locations:

- [README.md](/Users/dlemire/CVS/github/RoaringBitmap/README.md) (around lines 206-233, 442-452, 653-660): serialization/deserialization examples showing the required `validate()`-after-deserialize pattern for untrusted input, and the FAQ entry on the "bad cookie" error.
- `RoaringBitmap.validate()`, `Roaring64Bitmap.validate()`, `Roaring64NavigableMap.validate()`, and `ImmutableRoaringBitmap.validate()` in [roaringbitmap/src/main/java/org/roaringbitmap](/Users/dlemire/CVS/github/RoaringBitmap/roaringbitmap/src/main/java/org/roaringbitmap): the validation entry points for the various bitmap types.

The fuzzing harnesses under `fuzz-tests/` and the deserialization tests in the test suite exist precisely to ensure the deserializers and the validators behave correctly under adversarial input. A report that treats "deserializing attacker-controlled bytes and then using the result without calling `validate()`" as a bug in the deserializer is a misunderstanding of the stated API contract.

When triaging such reports, point to the validation requirement (`validate()`) documented in the README and close as "not a bug / user error / documented behavior."
