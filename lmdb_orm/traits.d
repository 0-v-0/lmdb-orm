module lmdb_orm.traits;

import std.meta;
import std.traits;

/// Check flags for column
enum CheckFlags {
	none,
	unique = 1,
	foreign = 2,
	all = CheckFlags.unique | CheckFlags.foreign,
}

/// Provide a custom name in the database for table
struct model {
	string name; /// The name of the table in the database
}

/// Get the name of the table in the database
template dbNameOf(T) {
	enum uda = getUDA!(T, model);
	static if (uda.length)
		enum dbNameOf = uda.name;
	else
		enum dbNameOf = T.stringof;
}

/// Mark a specific column as serial on the table
enum serial;

/// Mark a specific column as primary key on the table
enum PK;

/// Mark a specific column as unique on the table
enum unique;

/// Mark a specific column as non-empty on the table
enum nonEmpty;

/// foreign key
template foreign(alias field) {
}

alias getTables(modules...) = Filter!(isPOD, getSymbolsWith!(model, modules));

package(lmdb_orm):
enum isPOD(T) = __traits(isPOD, T);
enum isPK(alias x) = hasUDA!(x, PK);

template getUDA(alias sym, T) {
	static foreach (uda; __traits(getAttributes, sym))
		static if (is(typeof(getUDA) == void) && is(typeof(uda) == T))
			alias getUDA = uda;
	static if (is(typeof(getUDA) == void))
		alias getUDA = T.init;
}

alias getAttrs(alias symbol, string member) =
	__traits(getAttributes, __traits(getMember, symbol, member));

template getSymbolsWith(alias attr, symbols...) {
	import std.meta;

	template hasAttr(alias symbol, string name) {
		static if (is(typeof(getAttrs!(symbol, name))))
			static foreach (a; getAttrs!(symbol, name)) {
				static if (is(typeof(hasAttr) == void)) {
					static if (__traits(isSame, a, attr))
						enum hasAttr = true;
					else static if (__traits(isTemplate, attr)) {
						static if (is(typeof(a) == attr!A, A...))
							enum hasAttr = true;
					} else {
						static if (is(typeof(a) == attr))
							enum hasAttr = true;
					}
				}
			}
		static if (is(typeof(hasAttr) == void))
			enum hasAttr = false;
	}

	alias getSymbolsWith = AliasSeq!();
	static foreach (symbol; symbols) {
		static foreach (name; __traits(derivedMembers, symbol))
			static if (hasAttr!(symbol, name))
				getSymbolsWith = AliasSeq!(getSymbolsWith, __traits(getMember, symbol, name));
	}
}
