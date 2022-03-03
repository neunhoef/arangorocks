#include <docopt.h>
#include <stdlib.h>
#include <velocypack/AttributeTranslator.h>
#include <velocypack/Dumper.h>
#include <velocypack/Iterator.h>
#include <velocypack/Slice.h>

#include <array>
#include <cassert>
#include <fstream>
#include <iostream>
#include <locale>
#include <set>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/transaction_db.h"

constexpr char const* Version = "0.0.1";

arangodb::velocypack::AttributeTranslator attrTranslator;

void initAttributeTranslator() {
  // these attribute names will be translated into short integer values
  attrTranslator.add("_key", 1);
  attrTranslator.add("_rev", 2);
  attrTranslator.add("_id", 3);
  attrTranslator.add("_from", 4);
  attrTranslator.add("_to", 5);

  attrTranslator.seal();
}

struct CustomTypeHandler final : public VPackCustomTypeHandler {
  CustomTypeHandler() {}
  ~CustomTypeHandler() = default;

  void dump(VPackSlice const& value, VPackDumper* dumper,
            VPackSlice const& base) override final {
    dumper->appendString(toString(value, nullptr, base));
  }

  std::string toString(VPackSlice const& value, VPackOptions const* options,
                       VPackSlice const& base) override final {
    assert(value.isCustom() && value.head() == 0xf3);
    uint8_t const* p = value.start();
    uint64_t v = 0;
    for (size_t i = 8; i > 0; --i) {
      v = (v << 8) + p[i];
    }
    assert(base.isObject());
    p = base.begin() + base.findDataOffset(base.head());
    if (*p == 0x31) {  // This is `_key`
      // the + 1 is required so that we can skip over the attribute name
      // and point to the attribute value
      VPackSlice keySlice(p + 1);
      return std::to_string(v) + "/" + keySlice.copyString();
    }
    assert(base.hasKey("_key"));
    return std::to_string(v) + "/" + base.get("_key").copyString();
  }
};

CustomTypeHandler customTypeHandler;

enum class Family : std::size_t {
  Definitions = 0,
  Documents = 1,
  PrimaryIndex = 2,
  EdgeIndex = 3,
  VPackIndex = 4,  // persistent, "skiplist", "hash"
  GeoIndex = 5,
  FulltextIndex = 6,
  ReplicatedLogs = 7,
  ZkdIndex = 8,

  Invalid = 1024  // special placeholder
};

std::array<char const*, 9> FamilyNames = {
    "default",  "Documents",     "PrimaryIndex",   "EdgeIndex", "VPackIndex",
    "GeoIndex", "FulltextIndex", "ReplicatedLogs", "ZkdIndex"};

class RocksDBPrefixExtractor final : public rocksdb::SliceTransform {
 public:
  RocksDBPrefixExtractor() = default;
  ~RocksDBPrefixExtractor() = default;

  const char* Name() const override { return "RocksDBPrefixExtractor"; }

  rocksdb::Slice Transform(rocksdb::Slice const& key) const override {
    // 8-byte objectID + 0..n-byte string + 1-byte '\0'
    // + 8 byte revisionID + 1-byte 0xFF (these are for cut off)
    if (key.data()[key.size() - 1] != '\0') {
      // unfortunately rocksdb seems to call Tranform(Transform(k))
      size_t l = key.size() - sizeof(uint64_t) - sizeof(char);
      return rocksdb::Slice(key.data(), l);
    } else {
      return key;
    }
  }

  bool InDomain(rocksdb::Slice const& key) const override {
    // 8-byte objectID + n-byte string + 1-byte '\0' + ...
    return key.data()[key.size() - 1] != '\0';
  }

  bool InRange(rocksdb::Slice const& dst) const override {
    return dst.data()[dst.size() - 1] != '\0';
  }

  bool SameResultWhenAppended(rocksdb::Slice const& prefix) const override {
    return prefix.data()[prefix.size() - 1] == '\0';
  }
};

static int8_t const typeWeights[256] = {
    0 /* 0x00 */,   4 /* 0x01 */,  4 /* 0x02 */, 4 /* 0x03 */,  4 /* 0x04 */,
    4 /* 0x05 */,   4 /* 0x06 */,  4 /* 0x07 */, 4 /* 0x08 */,  4 /* 0x09 */,
    5 /* 0x0a */,   5 /* 0x0b */,  5 /* 0x0c */, 5 /* 0x0d */,  5 /* 0x0e */,
    5 /* 0x0f */,   5 /* 0x10 */,  5 /* 0x11 */, 5 /* 0x12 */,  4 /* 0x13 */,
    5 /* 0x14 */,   0 /* 0x15 */,  0 /* 0x16 */, -1 /* 0x17 */, 0 /* 0x18 */,
    1 /* 0x19 */,   1 /* 0x1a */,  2 /* 0x1b */, 2 /* 0x1c */,  -50 /* 0x1d */,
    -99 /* 0x1e */, 99 /* 0x1f */, 2 /* 0x20 */, 2 /* 0x21 */,  2 /* 0x22 */,
    2 /* 0x23 */,   2 /* 0x24 */,  2 /* 0x25 */, 2 /* 0x26 */,  2 /* 0x27 */,
    2 /* 0x28 */,   2 /* 0x29 */,  2 /* 0x2a */, 2 /* 0x2b */,  2 /* 0x2c */,
    2 /* 0x2d */,   2 /* 0x2e */,  2 /* 0x2f */, 2 /* 0x30 */,  2 /* 0x31 */,
    2 /* 0x32 */,   2 /* 0x33 */,  2 /* 0x34 */, 2 /* 0x35 */,  2 /* 0x36 */,
    2 /* 0x37 */,   2 /* 0x38 */,  2 /* 0x39 */, 2 /* 0x3a */,  2 /* 0x3b */,
    2 /* 0x3c */,   2 /* 0x3d */,  2 /* 0x3e */, 2 /* 0x3f */,  3 /* 0x40 */,
    3 /* 0x41 */,   3 /* 0x42 */,  3 /* 0x43 */, 3 /* 0x44 */,  3 /* 0x45 */,
    3 /* 0x46 */,   3 /* 0x47 */,  3 /* 0x48 */, 3 /* 0x49 */,  3 /* 0x4a */,
    3 /* 0x4b */,   3 /* 0x4c */,  3 /* 0x4d */, 3 /* 0x4e */,  3 /* 0x4f */,
    3 /* 0x50 */,   3 /* 0x51 */,  3 /* 0x52 */, 3 /* 0x53 */,  3 /* 0x54 */,
    3 /* 0x55 */,   3 /* 0x56 */,  3 /* 0x57 */, 3 /* 0x58 */,  3 /* 0x59 */,
    3 /* 0x5a */,   3 /* 0x5b */,  3 /* 0x5c */, 3 /* 0x5d */,  3 /* 0x5e */,
    3 /* 0x5f */,   3 /* 0x60 */,  3 /* 0x61 */, 3 /* 0x62 */,  3 /* 0x63 */,
    3 /* 0x64 */,   3 /* 0x65 */,  3 /* 0x66 */, 3 /* 0x67 */,  3 /* 0x68 */,
    3 /* 0x69 */,   3 /* 0x6a */,  3 /* 0x6b */, 3 /* 0x6c */,  3 /* 0x6d */,
    3 /* 0x6e */,   3 /* 0x6f */,  3 /* 0x70 */, 3 /* 0x71 */,  3 /* 0x72 */,
    3 /* 0x73 */,   3 /* 0x74 */,  3 /* 0x75 */, 3 /* 0x76 */,  3 /* 0x77 */,
    3 /* 0x78 */,   3 /* 0x79 */,  3 /* 0x7a */, 3 /* 0x7b */,  3 /* 0x7c */,
    3 /* 0x7d */,   3 /* 0x7e */,  3 /* 0x7f */, 3 /* 0x80 */,  3 /* 0x81 */,
    3 /* 0x82 */,   3 /* 0x83 */,  3 /* 0x84 */, 3 /* 0x85 */,  3 /* 0x86 */,
    3 /* 0x87 */,   3 /* 0x88 */,  3 /* 0x89 */, 3 /* 0x8a */,  3 /* 0x8b */,
    3 /* 0x8c */,   3 /* 0x8d */,  3 /* 0x8e */, 3 /* 0x8f */,  3 /* 0x90 */,
    3 /* 0x91 */,   3 /* 0x92 */,  3 /* 0x93 */, 3 /* 0x94 */,  3 /* 0x95 */,
    3 /* 0x96 */,   3 /* 0x97 */,  3 /* 0x98 */, 3 /* 0x99 */,  3 /* 0x9a */,
    3 /* 0x9b */,   3 /* 0x9c */,  3 /* 0x9d */, 3 /* 0x9e */,  3 /* 0x9f */,
    3 /* 0xa0 */,   3 /* 0xa1 */,  3 /* 0xa2 */, 3 /* 0xa3 */,  3 /* 0xa4 */,
    3 /* 0xa5 */,   3 /* 0xa6 */,  3 /* 0xa7 */, 3 /* 0xa8 */,  3 /* 0xa9 */,
    3 /* 0xaa */,   3 /* 0xab */,  3 /* 0xac */, 3 /* 0xad */,  3 /* 0xae */,
    3 /* 0xaf */,   3 /* 0xb0 */,  3 /* 0xb1 */, 3 /* 0xb2 */,  3 /* 0xb3 */,
    3 /* 0xb4 */,   3 /* 0xb5 */,  3 /* 0xb6 */, 3 /* 0xb7 */,  3 /* 0xb8 */,
    3 /* 0xb9 */,   3 /* 0xba */,  3 /* 0xbb */, 3 /* 0xbc */,  3 /* 0xbd */,
    3 /* 0xbe */,   3 /* 0xbf */,  3 /* 0xc0 */, 3 /* 0xc1 */,  3 /* 0xc2 */,
    3 /* 0xc3 */,   3 /* 0xc4 */,  3 /* 0xc5 */, 3 /* 0xc6 */,  3 /* 0xc7 */,
    2 /* 0xc8 */,   2 /* 0xc9 */,  2 /* 0xca */, 2 /* 0xcb */,  2 /* 0xcc */,
    2 /* 0xcd */,   2 /* 0xce */,  2 /* 0xcf */, 2 /* 0xd0 */,  2 /* 0xd1 */,
    2 /* 0xd2 */,   2 /* 0xd3 */,  2 /* 0xd4 */, 2 /* 0xd5 */,  2 /* 0xd6 */,
    2 /* 0xd7 */,   0 /* 0xd8 */,  0 /* 0xd9 */, 0 /* 0xda */,  0 /* 0xdb */,
    0 /* 0xdc */,   0 /* 0xdd */,  0 /* 0xde */, 0 /* 0xdf */,  0 /* 0xe0 */,
    0 /* 0xe1 */,   0 /* 0xe2 */,  0 /* 0xe3 */, 0 /* 0xe4 */,  0 /* 0xe5 */,
    0 /* 0xe6 */,   0 /* 0xe7 */,  0 /* 0xe8 */, 0 /* 0xe9 */,  0 /* 0xea */,
    0 /* 0xeb */,   0 /* 0xec */,  0 /* 0xed */, 0 /* 0xee */,  0 /* 0xef */,
    3 /* 0xf0 */,   3 /* 0xf1 */,  3 /* 0xf2 */, 3 /* 0xf3 */,  3 /* 0xf4 */,
    3 /* 0xf5 */,   3 /* 0xf6 */,  3 /* 0xf7 */, 3 /* 0xf8 */,  3 /* 0xf9 */,
    3 /* 0xfa */,   3 /* 0xfb */,  3 /* 0xfc */, 3 /* 0xfd */,  3 /* 0xfe */,
    3 /* 0xff */,
};

static inline int8_t TypeWeight(VPackSlice& slice) {
again:
  int8_t w = typeWeights[slice.head()];
  if (w == -50) {
    slice = slice.resolveExternal();
    goto again;
  }
  return w;
}

int compare(
    VPackSlice lhs, VPackSlice rhs, bool useUTF8,
    VPackOptions const* options = &arangodb::velocypack::Options::Defaults,
    VPackSlice const* lhsBase = nullptr, VPackSlice const* rhsBase = nullptr);

int compareNumberValues(VPackValueType lhsType, VPackSlice lhs,
                        VPackSlice rhs) {
  if (lhsType == rhs.type()) {
    // both types are equal
    if (lhsType == VPackValueType::Int || lhsType == VPackValueType::SmallInt) {
      // use exact comparisons. no need to cast to double
      int64_t l = lhs.getIntUnchecked();
      int64_t r = rhs.getIntUnchecked();
      if (l == r) {
        return 0;
      }
      return (l < r ? -1 : 1);
    }

    if (lhsType == VPackValueType::UInt) {
      // use exact comparisons. no need to cast to double
      uint64_t l = lhs.getUIntUnchecked();
      uint64_t r = rhs.getUIntUnchecked();
      if (l == r) {
        return 0;
      }
      return (l < r ? -1 : 1);
    }
    // fallthrough to double comparison
  }

  double l = lhs.getNumericValue<double>();
  double r = rhs.getNumericValue<double>();
  if (l == r) {
    return 0;
  }
  return (l < r ? -1 : 1);
}

int compareStringValues(char const* left, VPackValueLength nl,
                        char const* right, VPackValueLength nr, bool useUTF8) {
  int res;
  if (useUTF8) {
    std::locale l("");
    auto& f = std::use_facet<std::collate<char>>(l);
    res = f.compare(left, left + nl, right, right + nr);
  } else {
    size_t len = static_cast<size_t>(nl < nr ? nl : nr);
    res = memcmp(left, right, len);
  }

  if (res != 0) {
    return (res < 0 ? -1 : 1);
  }

  // res == 0
  if (nl == nr) {
    return 0;
  }
  // res == 0, but different string lengths
  return (nl < nr ? -1 : 1);
}

int compareUtf8(char const* left, size_t leftLength, char const* right,
                size_t rightLength) {
  std::locale l("");
  auto& f = std::use_facet<std::collate<char>>(l);
  return f.compare(left, left + leftLength, right, right + rightLength);
}

struct AttributeSorterUTF8StringView {
  bool operator()(std::string_view l, std::string_view r) const {
    return compareUtf8(l.data(), l.size(), r.data(), r.size()) < 0;
  }
};

struct AttributeSorterBinaryStringView {
  bool operator()(std::string_view l, std::string_view r) const noexcept {
    // use binary comparison of attribute names
    size_t cmpLength = (std::min)(l.size(), r.size());
    int res = memcmp(l.data(), r.data(), cmpLength);
    if (res < 0) {
      return true;
    }
    if (res == 0) {
      return l.size() < r.size();
    }
    return false;
  }
};

template <typename T>
static void unorderedKeys(VPackSlice const& slice, T& result) {
  arangodb::velocypack::ObjectIterator it(slice, true);

  while (it.valid()) {
    arangodb::velocypack::ValueLength l;
    char const* p = it.key(true).getString(l);
    result.emplace(p, l);
    it.next();
  }
}

template <bool useUtf8, typename Comparator>
int compareObjects(VPackSlice lhs, VPackSlice rhs,
                   VPackOptions const* options) {
  // compare two velocypack objects
  std::set<std::string_view, Comparator> keys;
  unorderedKeys(lhs, keys);
  unorderedKeys(rhs, keys);
  for (auto const& key : keys) {
    VPackSlice lhsValue = lhs.get(key).resolveExternal();
    if (lhsValue.isNone()) {
      // not present => null
      lhsValue = VPackSlice::nullSlice();
    }
    VPackSlice rhsValue = rhs.get(key).resolveExternal();
    if (rhsValue.isNone()) {
      // not present => null
      rhsValue = VPackSlice::nullSlice();
    }

    int result = compare(lhsValue, rhsValue, useUtf8, options, &lhs, &rhs);

    if (result != 0) {
      return result;
    }
  }

  return 0;
}

int compare(VPackSlice lhs, VPackSlice rhs, bool useUTF8,
            VPackOptions const* options, VPackSlice const* lhsBase,
            VPackSlice const* rhsBase) {
  {
    // will resolve externals and modify both lhs & rhs...
    int8_t lWeight = TypeWeight(lhs);
    int8_t rWeight = TypeWeight(rhs);

    if (lWeight != rWeight) {
      return (lWeight < rWeight ? -1 : 1);
    }
  }

  // lhs and rhs have equal weights

  // note that the following code would be redundant because it was already
  // checked that lhs & rhs have the same TypeWeight, which is 0 for none.
  //  and for TypeWeight 0 we always return value 0
  // if (lhs.isNone() || rhs.isNone()) {
  //   // if rhs is none. we cannot be sure here that both are nones.
  //   // there can also exist the situation that lhs is a none and rhs is a
  //   // null value
  //   // (or vice versa). Anyway, the compare value is the same for both,
  //   return 0;
  // }

  auto lhsType = lhs.type();

  switch (lhsType) {
    case VPackValueType::Null:
      return 0;
    case VPackValueType::Bool: {
      bool left = lhs.isTrue();
      if (left == rhs.isTrue()) {
        return 0;
      }
      if (!left) {
        return -1;
      }
      return 1;
    }
    case VPackValueType::Double:
    case VPackValueType::Int:
    case VPackValueType::UInt:
    case VPackValueType::SmallInt: {
      return compareNumberValues(lhsType, lhs, rhs);
    }
    case VPackValueType::String:
    case VPackValueType::Custom: {
      VPackValueLength nl;
      VPackValueLength nr;
      char const* left;
      char const* right;
      std::string lhsString;
      std::string rhsString;

      if (lhs.isCustom()) {
        if (lhsBase == nullptr || options == nullptr ||
            options->customTypeHandler == nullptr) {
          throw std::string("Could not extract custom attribute.");
        }
        lhsString =
            options->customTypeHandler->toString(lhs, options, *lhsBase);
        left = lhsString.data();
        nl = lhsString.size();
      } else {
        left = lhs.getStringUnchecked(nl);
      }

      if (rhs.isCustom()) {
        if (rhsBase == nullptr || options == nullptr ||
            options->customTypeHandler == nullptr) {
          throw std::string("Could not extract custom attribute.");
        }
        rhsString =
            options->customTypeHandler->toString(rhs, options, *rhsBase);
        right = rhsString.data();
        nr = rhsString.size();
      } else {
        right = rhs.getStringUnchecked(nr);
      }

      return compareStringValues(left, nl, right, nr, useUTF8);
    }
    case VPackValueType::Array: {
      VPackArrayIterator al(lhs);
      VPackArrayIterator ar(rhs);

      VPackValueLength const n = (std::max)(al.size(), ar.size());
      for (VPackValueLength i = 0; i < n; ++i) {
        VPackSlice lhsValue;
        VPackSlice rhsValue;

        if (i < al.size()) {
          lhsValue = al.value();
          al.next();
        }
        if (i < ar.size()) {
          rhsValue = ar.value();
          ar.next();
        }

        int result = compare(lhsValue, rhsValue, useUTF8, options, &lhs, &rhs);
        if (result != 0) {
          return result;
        }
      }

      return 0;
    }
    case VPackValueType::Object: {
      if (useUTF8) {
        return ::compareObjects<true, AttributeSorterUTF8StringView>(lhs, rhs,
                                                                     options);
      }
      return ::compareObjects<false, AttributeSorterBinaryStringView>(lhs, rhs,
                                                                      options);
    }
    case VPackValueType::Illegal:
    case VPackValueType::MinKey:
    case VPackValueType::MaxKey:
    case VPackValueType::None:
      // uncommon cases are compared at the end
      return 0;
    default:
      // Contains all other ValueTypes of VelocyPack.
      // They are not used in ArangoDB so this cannot occur
      return 0;
  }
}

int compareIndexedValues(arangodb::velocypack::Slice const& lhs,
                         arangodb::velocypack::Slice const& rhs) {
  arangodb::velocypack::ArrayIterator lhsIter(lhs);
  arangodb::velocypack::ArrayIterator rhsIter(rhs);

  do {
    bool lhsValid = lhsIter.valid();
    bool rhsValid = rhsIter.valid();

    if (!lhsValid && !rhsValid) {
      return static_cast<int>(lhsIter.size() - rhsIter.size());
    }

    int res = compare((lhsValid ? *lhsIter : VPackSlice::noneSlice()),
                      (rhsValid ? *rhsIter : VPackSlice::noneSlice()), true);
    if (res != 0) {
      return res;
    }

    ++lhsIter;
    ++rhsIter;
  } while (true);
}

int compareIndexValues(rocksdb::Slice const& lhs, rocksdb::Slice const& rhs) {
  constexpr size_t objectIDLength = sizeof(uint64_t);

  int r = memcmp(lhs.data(), rhs.data(), objectIDLength);

  if (r != 0) {
    // different object ID
    return r;
  }

  if (lhs.size() == objectIDLength || rhs.size() == objectIDLength) {
    if (lhs.size() == rhs.size()) {
      return 0;
    }
    return ((lhs.size() < rhs.size()) ? -1 : 1);
  }

  VPackSlice const lSlice = VPackSlice(
      reinterpret_cast<uint8_t const*>(lhs.data()) + sizeof(uint64_t));
  VPackSlice const rSlice = VPackSlice(
      reinterpret_cast<uint8_t const*>(rhs.data()) + sizeof(uint64_t));

  r = compareIndexedValues(lSlice, rSlice);

  if (r != 0) {
    // comparison of index values produced an unambiguous result
    return r;
  }

  // index values were identical. now compare the leftovers (which is the
  // LocalDocumentId for non-unique indexes)

  constexpr size_t offset = sizeof(uint64_t);
  size_t const lOffset = offset + static_cast<size_t>(lSlice.byteSize());
  size_t const rOffset = offset + static_cast<size_t>(rSlice.byteSize());
  size_t const lSize = lhs.size() - lOffset;
  size_t const rSize = rhs.size() - rOffset;
  size_t minSize = std::min(lSize, rSize);

  if (minSize > 0) {
    char const* lBase = lhs.data() + lOffset;
    char const* rBase = rhs.data() + rOffset;
    r = memcmp(lBase, rBase, minSize);
    if (r != 0) {
      return r;
    }
  }

  return static_cast<int>(lSize - rSize);
}

class RocksDBVPackComparator final : public rocksdb::Comparator {
 public:
  RocksDBVPackComparator() = default;
  ~RocksDBVPackComparator() = default;

  /// @brief Compares any two RocksDB keys.
  /// returns  < 0 if lhs < rhs
  ///          > 0 if lhs > rhs
  ///            0 if lhs == rhs
  int Compare(rocksdb::Slice const& lhs,
              rocksdb::Slice const& rhs) const override {
    return compareIndexValues(lhs, rhs);
  }

  bool Equal(rocksdb::Slice const& lhs,
             rocksdb::Slice const& rhs) const override {
    return compareIndexValues(lhs, rhs) == 0;
  }

  // SECTION: API compatibility
  char const* Name() const override { return "RocksDBVPackComparator"; }
  void FindShortestSeparator(std::string*,
                             rocksdb::Slice const&) const override {}
  void FindShortSuccessor(std::string*) const override {}

 private:
  /// @brief Compares two IndexValue keys or two UniqueIndexValue keys
  /// (containing VelocyPack data and more).
  // int compareIndexValues(rocksdb::Slice const& lhs,
  //                     rocksdb::Slice const& rhs) const;
};

RocksDBVPackComparator vpackCmp;

std::vector<rocksdb::ColumnFamilyHandle*> cfHandles;

rocksdb::TransactionDB* openDatabase(std::string const& path) {
  constexpr size_t objectIDLength = sizeof(uint64_t);
  cfHandles.clear();
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions tableOptions;
  rocksdb::TransactionDBOptions transactionOptions;
  rocksdb::TransactionDB* db;
  std::vector<rocksdb::ColumnFamilyDescriptor> cfFamilies;

  auto addFamily = [&options, &tableOptions, &cfFamilies](Family family) {
    rocksdb::ColumnFamilyOptions specialized(options);
    switch (family) {
      case Family::Definitions:
      case Family::Invalid:
        break;
      case Family::Documents:
        specialized.optimize_filters_for_hits = true;
        [[fallthrough]];
      case Family::PrimaryIndex:
      case Family::GeoIndex:
      case Family::FulltextIndex:
      case Family::ZkdIndex:
      case Family::ReplicatedLogs: {
        // fixed 8 byte object id prefix
        specialized.prefix_extractor =
            std::shared_ptr<rocksdb::SliceTransform const>(
                rocksdb::NewFixedPrefixTransform(objectIDLength));
        break;
      }
      case Family::EdgeIndex: {
        specialized.prefix_extractor =
            std::make_shared<RocksDBPrefixExtractor>();
        // also use hash-search based SST file format
        rocksdb::BlockBasedTableOptions to(tableOptions);
        to.index_type = rocksdb::BlockBasedTableOptions::IndexType::kHashSearch;
        specialized.table_factory = std::shared_ptr<rocksdb::TableFactory>(
            rocksdb::NewBlockBasedTableFactory(to));
        break;
      }
      case Family::VPackIndex: {
        rocksdb::BlockBasedTableOptions tableOpts(tableOptions);
        tableOpts.filter_policy.reset();  // intentionally no bloom filter here
        specialized.table_factory = std::shared_ptr<rocksdb::TableFactory>(
            rocksdb::NewBlockBasedTableFactory(tableOpts));
        specialized.comparator = &vpackCmp;
        break;
      }
    }
    std::string name = FamilyNames[(std::size_t)family];
    cfFamilies.emplace_back(name, specialized);
  };
  // no prefix families for default column family (Has to be there)
  addFamily(Family::Definitions);
  addFamily(Family::Documents);
  addFamily(Family::PrimaryIndex);
  addFamily(Family::EdgeIndex);
  addFamily(Family::VPackIndex);
  addFamily(Family::GeoIndex);
  addFamily(Family::FulltextIndex);
  addFamily(Family::ReplicatedLogs);
  addFamily(Family::ZkdIndex);

  options.create_if_missing = true;

  rocksdb::Status status = rocksdb::TransactionDB::Open(
      options, transactionOptions, path, cfFamilies, &cfHandles, &db);
  if (!status.ok()) {
    std::cerr << "Could not open database, error: " << status.ToString()
              << std::endl;
    return nullptr;
  }
  return db;
}

void closeDatabase(rocksdb::TransactionDB* db) {
  for (size_t i = 0; i < cfHandles.size(); ++i) {
    db->DestroyColumnFamilyHandle(cfHandles[i]);
  }
  db->Close();
  delete db;
}

char hexChar(uint8_t x) {
  if (x <= 9) {
    return x + '0';
  } else {
    return x - 10 + 'A';
  }
}

void hexDump(rocksdb::Slice const& sl, std::string& a, std::string& b) {
  char const* p = sl.data();
  size_t len = sl.size() * 2 + 1;
  char* buffer = new char[len];
  char* buffer2 = new char[len];
  char* q = buffer;
  char* q2 = buffer2;
  for (size_t i = 0; i < sl.size(); ++i) {
    uint8_t c = (uint8_t)*p++;
    *q++ = hexChar(c >> 4);
    *q++ = hexChar(c & 0xf);
    *q2++ = ' ';
    if (c <= 32 || c >= 127) {
      *q2++ = '.';
    } else {
      *q2++ = c;
    }
  }
  a.append(buffer, len - 1);
  b.append(buffer2, len - 1);
  delete[] buffer;
  delete[] buffer2;
}

void dump_all(rocksdb::TransactionDB* db, std::string const& outfile) {
  rocksdb::WriteOptions opts;
  rocksdb::TransactionOptions topts;
  rocksdb::Transaction* trx = db->BeginTransaction(opts, topts);
  rocksdb::ReadOptions ropts;
  std::ofstream out(outfile.c_str(), std::ios::out);
  bool doOutput = true;
  if (outfile == "/dev/null") {
    doOutput = false;
  }
  for (size_t f = (size_t)Family::Definitions; f <= (size_t)Family::ZkdIndex;
       ++f) {
    std::cout << "Column family " << f << " with name " << FamilyNames[f]
              << "...\n";
    if (doOutput) {
      out << "Column family " << f << " with name " << FamilyNames[f] << ":\n";
    }
    rocksdb::Iterator* it = trx->GetIterator(ropts, cfHandles[f]);
    rocksdb::Slice empty;
    it->Seek(empty);
    std::string line1;
    std::string line2;
    while (it->Valid()) {
      rocksdb::Slice key = it->key();
      rocksdb::Slice value = it->value();
      line1.clear();
      line2.clear();
      line1.reserve(2 * key.size() + 2 * value.size() + 5);
      line2.reserve(2 * key.size() + 2 * value.size() + 5);
      hexDump(key, line1, line2);
      line1.append(" -> ");
      line2.append("    ");
      hexDump(value, line1, line2);
      if (doOutput) {
        out << line1 << "\n" << line2 << "\n";
      }
      it->Next();
    }
    delete it;
  }
  delete trx;
}

void appendBigEndian(std::string& s, uint64_t v) {
  uint64_t shift = 56;
  s.reserve(s.size() + 8);
  for (size_t i = 0; i < 8; ++i) {
    s.push_back((uint8_t)((v >> shift) & 0xffull));
    shift -= 8;
  }
}

void dump_collection(rocksdb::TransactionDB* db, uint64_t objid,
                     std::string const& outfile) {
  rocksdb::WriteOptions opts;
  rocksdb::TransactionOptions topts;
  rocksdb::Transaction* trx = db->BeginTransaction(opts, topts);
  rocksdb::ReadOptions ropts;
  std::ofstream out(outfile.c_str(), std::ios::out);
  out << "Dumping collection with objid " << objid
      << " directly from documents family:\n";
  std::string startKey;
  appendBigEndian(startKey, objid);
  std::string endKey;
  appendBigEndian(endKey, objid + 1);
  rocksdb::Slice start(startKey);
  rocksdb::Slice end(endKey);
  ropts.iterate_upper_bound = &end;
  rocksdb::Iterator* it =
      trx->GetIterator(ropts, cfHandles[(size_t)Family::Documents]);
  it->Seek(start);
  std::string line1;
  std::string line2;
  VPackOptions vopt;
  vopt.attributeTranslator = &attrTranslator;
  VPackOptions::Defaults.attributeTranslator = &attrTranslator;
  vopt.unsupportedTypeBehavior = arangodb::velocypack::Options::
      UnsupportedTypeBehavior::ConvertUnsupportedType;
  vopt.customTypeHandler = &customTypeHandler;
  bool doOutput = true;
  if (outfile == "/dev/null") {
    doOutput = false;
  }
  while (it->Valid()) {
    rocksdb::Slice value = it->value();
    VPackSlice slice((uint8_t*)value.data());
    if (slice.byteSize() != value.size()) {
      std::cerr << "Value size is not byteSize of slice!\n";
    }
    if (doOutput) {
      out << slice.toJson(&vopt) << "\n";
    }
    it->Next();
  }
  delete it;
  delete trx;
}

static const char USAGE[] =
    R"(arangorocks.

  Usage:
    arangorocks [-h] [--version] [-d DBPATH] dump_all [-o OUTPATH]
    arangorocks [-h] [--version] [-d DBPATH] dump_collection [-c OBJID] [-o OUTPATH]

  Options:
    -h --help                    Only show this screen.
    --version                    Only show the version.
    -c OBJID --objid=OBJID       ObjectId of collection to dump.
    -d DBPATH --dbpath=DBPATH    Path to DB [default: /tmp/testdb].
    -o OUTPATH --output=OUTPATH  Path to write collection dump [default: ./dump].

)";

int main(int argc, char** argv) {
  initAttributeTranslator();
  std::map<std::string, docopt::value> args =
      docopt::docopt(USAGE, {argv + 1, argv + argc},
                     true,  // show help if requested
                     std::string("arangorocks ") + std::string(Version));
  std::string dbpath = args["--dbpath"].asString();
  std::cout << "Using database path " << dbpath << "..." << std::endl;

  if (args["dump_all"].asBool()) {
    std::string outfile = args["--output"].asString();
    std::cout << "Dumping all to file " << outfile << "." << std::endl;
    rocksdb::TransactionDB* db = openDatabase(dbpath);
    if (db == nullptr) {
      return 1;
    }
    dump_all(db, outfile);
    closeDatabase(db);
  } else if (args["dump_collection"].asBool()) {
    if (!args["--objid"].isString()) {
      std::cerr << "Need --objid argument! Giving up." << std::endl;
      return 2;
    }
    uint64_t objId = atol(args["--objid"].asString().c_str());
    std::string outfile = args["--output"].asString();
    std::cout << "Dumping collection with ObjectId " << objId << " to file "
              << outfile << "." << std::endl;
    rocksdb::TransactionDB* db = openDatabase(dbpath);
    if (db == nullptr) {
      return 1;
    }
    dump_collection(db, objId, outfile);
    closeDatabase(db);

  } else {
    std::cout << "Nothing do do." << std::endl;
  }

  return 0;
}
