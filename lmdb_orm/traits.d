module lmdb_orm.traits;

import std.meta;
import std.traits;
import lmdb_orm.oo;
import lmdb_orm.lmdb : MDB_txn;

version (LDC)
	import ldc.attributes;
else
	private enum restrict;

/// Check flags for column
enum CheckFlags {
	none,
	unique = 1,
	foreignTo = 2,
	foreignFrom = 4,
	foreign = foreignTo | foreignFrom,
	empty = 8,
	all = CheckFlags.unique | CheckFlags.foreign | CheckFlags.empty,
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
	ulong max = long.max; /// The maximum value of the serial
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
				static assert(getSerial.max <= typeof(f).max, "Serial max value is too large");
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
template FK(alias field) {
	static assert(isPOD!(__traits(parent, field)), "Field must be a column");
	static assert(isPK!field || hasUDA!(field, unique), "Foreign key must be primary key or unique");
	// TODO: check compound primary key
	struct FK;
}

alias getTables(modules...) = Filter!(isPOD, getSymbolsWith!(model, modules));

enum isPK(alias x) = hasUDA!(x, PK) || hasUDA!(x, serial);

alias Save = void delegate() @safe;
alias Del = bool delegate() @safe;

package:

template Call(alias func, args...) {
	static if (is(typeof(func(args)) == void))
		int _ = { func(args); return 0; }();
	else
		const ret = func(args);
}

template CallNext(T, string name, args...) {
	static if (is(typeof(__traits(getMember, T, name)(args)))) {
		alias func = __traits(getMember, T, name);
		static foreach (sc; __traits(getParameterStorageClasses, func(args), 1)) {
			static if (sc == "scope")
				enum isScope = true;
		}
		static assert(is(typeof(isScope)), "The first parameter of " ~
				fullyQualifiedName!func ~ " must be scope");
		mixin Call!(func, args);
	} else static if (is(typeof(args[1]())))
		mixin Call!(args[1]);
}

union Arr {
	import lmdb_orm.lmdb;

	Val v;
	MDB_val m;
	size_t[2] s;
}

struct Tuple(T...) {
	T expand;
}

/** Get the number of primary keys of the table
Params:
	T: the table
*/
template keyCount(T) if (isPOD!T) {
	static foreach_reverse (i, alias x; T.tupleof) {
		static if (is(typeof(keyCount) == void)) {
			static if (isPK!x)
				enum keyCount = i + 1;
		} else
			static assert(isPK!x, "Primary keys must be consecutive");
	}
	static if (is(typeof(keyCount) == void))
		enum keyCount = 0;
}

unittest {
	static assert(keyCount!User == 1);
	static assert(keyCount!Company == 1);
	static assert(keyCount!Relation == 2);
}

/// Get the type of the primary keys of the table
alias TKey(T) = typeof(T.tupleof[0 .. keyCount!T]);

/// threshold for inlining arrays, default 64
// TODO
enum lengthThreshold = size_t.max;

enum isFixedSize(T...) = !anySatisfy!(isDynamicArray, T);

alias Intern = void function(MDB_txn*, ref Val) @safe;

/// Get the size of the serialized object
void setSize(bool key = false, T)(@restrict ref Arr a, @restrict ref T obj, @restrict MDB_txn* txn, Intern intern = null) {
	static if (key) {
		enum start = 0;
		enum end = keyCount!T;
	} else {
		enum start = keyCount!T;
		enum end = T.tupleof.length;
	}
	alias args = AliasSeq!(obj.tupleof[start .. end]);
	static if (isFixedSize!(typeof(args))) {
		a.m.mv_size = Tuple!(typeof(args)).sizeof;
	} else {
		a.m.mv_size = byteLen!(start, end)(obj, txn, intern);
	}
}

/// Get the size of the serialized object
size_t byteLen(size_t start, size_t end, T)(@restrict ref T obj, @restrict MDB_txn* txn, Intern intern = null) {
	size_t size = Tuple!(typeof(T.tupleof[start .. end])).sizeof;
	foreach (i, ref x; obj.tupleof[start .. end]) {
		static if (isDynamicArray!(typeof(x))) {
			if (x.length < lengthThreshold) // inline
				size += x.length * typeof(x[0]).sizeof;
			else if (intern)
				intern(txn, *cast(Val*)&x);
		}
	}
	return size;
}

unittest {
	auto u = User(1, "foo", 1);
	assert(byteLen!(1, User.tupleof.length)(u, null) == 16 + 8 * 3 + "foo".length);
	auto c = Company(1, "bar", "address");
	assert(byteLen!(0, 1)(c, null) == 8);
	auto r = Relation(1, 2, RelationType.friend);
	assert(byteLen!(0, 2)(r, null) == 8 + 8);
	assert(byteLen!(2, 3)(r, null) == 4);
}

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
		enum UDAof = UDA();
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

version (unittest)  : import std.stdio;
import lmdb_orm.orm;

alias DB = FSDB!(lmdb_orm.traits);

@model
struct User {
	@serial long id;
	@unique @nonEmpty string name;
	@FK!(Company.id) long companyID;
	long createdAt;
	long updatedAt;

	static void onSave(T)(ref T user, scope Save next) {
		if (!user.createdAt)
			user.createdAt = now();
		user.updatedAt = now();
		next();
	}
}

@model
struct Company {
	@serial long id;
	@unique @nonEmpty string name;
	string address;

	static bool onDelete(long id, scope Del next) {
		writeln("Company ", id, " is deleted");
		return next();
	}
}

enum RelationType {
	friend,
	colleague,
	enemy,
}

@model
struct Relation {
	@PK @FK!(User.id) {
		long userA;
		long userB;
	}
	RelationType type;

	static void onSave(T)(ref T r, scope Next next) {
		if (r.type < RelationType.min || r.type > RelationType.max)
			throw new Exception("Invalid relation type");

		next();
	}
}

@property auto now() {
	import std.datetime;

	try {
		return Clock.currStdTime;
	} catch (Exception) {
		return long.max;
	}
}
