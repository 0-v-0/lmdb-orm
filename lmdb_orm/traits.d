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

enum isPK(alias x) = hasUDA!(x, PK);

package(lmdb_orm):

/// threshold for inlining arrays, default 64
// TODO
enum lengthThreshold = size_t.max;

/// Get the size of the serialized object, 0 if it is dynamic
size_t byteLen(T, alias filter = True)() {
	size_t size;
	foreach (alias x; T.tupleof) {
		static if (filter!x) {
			static if (isArray!(typeof(x))) {
				static assert(!hasIndirections!(typeof(x[0])), "not implemented");
				static if (isDynamicArray!(typeof(x))) {
					return 0;
				} else {
					size += x.length * typeof(x[0]).sizeof;
				}
			} else
				size += x.sizeof;
		}
	}
	return size;
}

unittest {
	import lmdb_orm.orm;

	static assert(byteLen!(User) == 0);
	static assert(byteLen!(Company) == 0);
	static assert(byteLen!(Relation) == 8 + 8 + 4);
	static assert(byteLen!(Relation, isPK) == 8 + 8);
}

/// Get the size of the serialized object
size_t byteLen(alias filter, alias intern, T)(ref T obj) {
	size_t size;
	foreach (ref x; obj.tupleof) {
		static if (filter!x) {
			static if (isArray!(typeof(x))) {
				static assert(!hasIndirections!(typeof(x[0])), "not implemented");
				static if (isStaticArray!(typeof(x))) {
					size += x.length * typeof(x[0]).sizeof;
				} else {
					if (x.length < lengthThreshold) // inline
						size += size_t.sizeof + x.length * typeof(x[0]).sizeof;
					else {
						intern(x);
						size += Val.sizeof;
					}
				}
			} else
				size += x.sizeof;
		}
	}
	return size;
}

enum True(alias x) = true;
enum isPOD(T) = __traits(isPOD, T);

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

bool likely(bool exp) {
	version (LDC) {
		import ldc.intrinsics;

		return llvm_expect(exp, true);
	} else
		return exp;
}

bool unlikely(bool exp) {
	version (LDC) {
		import ldc.intrinsics;

		return llvm_expect(exp, false);
	} else
		return exp;
}
