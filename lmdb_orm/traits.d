module lmdb_orm.traits;

import std.meta;
import std.traits;
import lmdb_orm.oo;

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
	DBFlags flags; /// The flags of the table in the database
}

/// Get the model attribute of the table
enum modelOf(T) = UDAof!(T, model);

unittest {
	@model("Class")
	static struct S {
		string name;
	}

	static assert(modelOf!(User).name == "User");
	static assert(modelOf!(Company).name == "Company");
	static assert(modelOf!(S).name == "Class");
	static assert(modelOf!(model).name == "");
}

/// Mark a specific column as serial on the table
struct serial {
	enum invalid = serial(0, 0, 0);

	ulong min = 1; /// The minimum value of the serial
	ulong max = ulong.max; /// The maximum value of the serial
	ulong step = 1; /// The step of the serial
}

/// Get the serial of the table
template getSerial(alias x) {
	alias udas = getUDAValues!(x, serial);
	static assert(udas.length < 2, "Only one " ~ serial.stringof ~ " is allowed for " ~ x.stringof);
	static if (udas.length) {
		alias getSerial = udas[0];
		enum index = -1;
	}
	static if (is(x)) {
		static foreach (i, alias f; x.tupleof) {
			static if (getSerial!f != serial.invalid) {
				static assert(i == 0, "Serial column must be the first column");
				static assert(isNumeric!(typeof(f)), "Serial column must be numeric");
				enum getSerial = .getSerial!f;
				static assert(getSerial.min < getSerial.max, "Invalid serial range");
				static assert(getSerial.max < typeof(f).max, "Serial max value is too large");
				enum index = i;
			}
		}
	}
	static if (is(typeof(getSerial) == void))
		enum getSerial = serial.invalid;
}

unittest {
	import lmdb_orm.orm;

	@serial
	static struct S {
		string name;
	}

	@serial
	static struct Multi {
		@serial int x;
	}

	static struct TypeMismatch {
		@serial string x;
	}

	static assert(getSerial!(S) == serial());
	static assert(!is(typeof(getSerial!(Multi))));
	static assert(!is(typeof(getSerial!(TypeMismatch))));
	static assert(getSerial!(User) == serial());
	static assert(getSerial!(Company) == serial());
	static assert(getSerial!(Relation) == serial.invalid);
}

/// Mark a specific column as primary key on the table
enum PK;

/// Mark a specific column as unique on the table
struct unique {
	string name; /// The name of the unique index
}

/// Mark a specific column as non-empty on the table
enum nonEmpty;

/// foreign key
template foreign(alias field) {
}

alias getTables(modules...) = Filter!(isPOD, getSymbolsWith!(model, modules));

enum isPK(alias x) = hasUDA!(x, PK) || hasUDA!(x, serial);

alias Next = void delegate() @safe;

package:

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
	import lmdb_orm.oo;

	size_t size;
	foreach (i, ref x; obj.tupleof) {
		static if (filter!(T.tupleof[i])) {
			static if (isArray!(typeof(x))) {
				static assert(!hasIndirections!(typeof(x[0])), "not implemented");
				static if (is(typeof(x) == E[], E)) {
					static assert(!isMutable!E, "Element type of " ~ fullyQualifiedName!(
							T.tupleof[i]) ~ " must be immutable");
					if (x.length < lengthThreshold) // inline
						size += size_t.sizeof + x.length * typeof(x[0]).sizeof;
					else {
						intern(*cast(Val*)&x);
						size += Val.sizeof;
					}
				} else
					size += x.sizeof;
			} else
				size += x.sizeof;
		}
	}
	return size;
}

enum True(alias x) = true;
enum isPOD(T) = __traits(isPOD, T);

template getUDAValues(alias x, UDA, UDA defaultVal = UDA.init) {
	template toVal(alias uda) {
		static if (is(uda))
			enum toVal = defaultVal;
		else
			enum toVal = uda;
	}

	alias getUDAValues = staticMap!(toVal, getUDAs!(x, UDA));
}

template UDAof(alias x, UDA, UDA defaultVal = UDA(x.stringof)) {
	alias udas = getUDAValues!(x, UDA, defaultVal);
	static assert(udas.length < 2, "Only one " ~ UDA.stringof ~ " is allowed for " ~ x.stringof);
	static if (udas.length)
		enum UDAof = udas[0];
	else
		enum UDAof = defaultVal;
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

pure @nogc @safe {
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
}

version (unittest):
import std.stdio;
import lmdb_orm.orm;

alias DB = FSDB!(lmdb_orm.traits);

@model
struct User {
	@serial long id;
	@unique string name;
	@foreign!(Company.id) long companyID;
	long createdAt;
	long updatedAt;

	void onSave(scope Next next) {
		if (next) {
			updatedAt = now();
			next();
		}
		createdAt = now();
	}
}

@model
struct Company {
	@serial long id;
	@unique string name;

	void onDelete(scope Next next) {
		writeln("Company ", id, " is deleted");
		next();
	}
}

enum RelationType {
	friend,
	colleague,
	enemy,
}

@model
struct Relation {
	@PK @foreign!(User.id) long userA;
	@PK @foreign!(User.id) long userB;
	RelationType type;

	void onSave(scope Next next) {
		if (type < RelationType.min || type > RelationType.max)
			throw new Exception("Invalid relation type");

		next();
	}
}

@property auto now() {
	import std.datetime;

	try {
		return Clock.currStdTime;
	} catch (Exception) {
		return 0;
	}
}
