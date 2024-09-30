module lmdb_orm.index;

import lmdb_orm.oo;
import lmdb_orm.traits;

template Index(string name) {
	@model(name)
	struct Index {
		@PK Val key;
		Val val;
	}
}

template UniqueIndices(Tables...) {
	import std.meta;

	alias UniqueIndices = AliasSeq!();
	static foreach (T; Tables) {
		static foreach (alias x; T.tupleof) {
			static if (getName!(x, unique)) {
				UniqueIndices = AliasSeq!(UniqueIndices,
					Index!(dbNameOf!T ~ "." ~ getName!(x, unique)));
			}
		}
	}
}
