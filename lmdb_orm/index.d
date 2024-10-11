module lmdb_orm.index;

import lmdb_orm.oo;
import lmdb_orm.traits;

/// Index
template Index(string name) {
	@model(name)
	struct Index {
		@PK Val key;
		Val val;
	}
}

/// Unique index
template UniqueIndices(Tables...) {
	import std.meta;

	alias UniqueIndices = AliasSeq!();
	static foreach (T; Tables) {
		static foreach (alias x; T.tupleof) {
			static if (UDAof!(x, unique)) {
				UniqueIndices = AliasSeq!(UniqueIndices,
					Index!(modelOf!T.name ~ "." ~ UDAof!(x, unique).name));
			}
		}
	}
}

/// Inverted index
template InvIndex(string name, alias x) if (getSerial!(__traits(parent, x)) != serial.invalid) {
	@model(name, DBFlags.dupFixed)
	struct InvIndex {
		@PK string key;
		T val;
	}
}
