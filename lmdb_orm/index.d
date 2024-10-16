module lmdb_orm.index;

import lmdb_orm.oo;
import lmdb_orm.traits;

package template UniqueIndices(Tables...) {
	import std.meta;
	import std.traits;

	alias UniqueIndices = AliasSeq!();
	static foreach (T; Tables) {
		static foreach (alias x; T.tupleof) {
			static if (UDAof!(x, unique).name.length) {
				static assert(!hasUDA!(x, PK), "Primary key cannot be unique");
				UniqueIndices = AliasSeq!(UniqueIndices, UniqueIndex!(T, x));
			}
		}
	}
}

/// Unique index
template UniqueIndex(T, alias x){
	@model(modelOf!T.name ~ "." ~ UDAof!(x, unique).name)
	struct UniqueIndex {
		@PK typeof(x) key;
		TKey!T val;
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
