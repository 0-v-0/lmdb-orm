module lmdb_orm.orm;

import lmdb_orm.lmdb : MDB_val, MDB_KEYEXIST;
import lmdb_orm.oo;
import lmdb_orm.traits;
import std.meta;
import std.traits;

import core.stdc.stdlib;
import core.stdc.string;

alias Next = void delegate();
private:
@model(metaDbName)
struct Meta;

template Index(string name) {
	@model(name)
	struct Index {
		@PK Val key;
		Val val;
	}
}

template UniqueIndices(Tables...) {
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

string checkDBs(DBs...)() {
	if (__ctfe) {
		enum nameOf(T) = T.stringof;

		size_t[string] indices;
		foreach (i, T; DBs) {
			enum name = dbNameOf!T;
			if (name in indices)
				return "Table " ~ T.stringof ~ " has the same name as " ~
					[staticMap!(nameOf, DBs)][indices[name]];
			indices[name] = i;
		}
	}
	return null;
}

union Arr {
	Val v;
	MDB_val m;
}

@property ref mark(MDB_val m)
	=> (cast(ubyte*)m.mv_data)[m.mv_size - 1];

/// name of the meta database
enum metaDbName = "#meta";
//static assert(__traits(getParameterStorageClasses, foo, 0)[0] == "scope");

/// Fixed-Schema Database
struct FSDB(modules...) {
	alias Tables = getTables!modules;
	/// Maximum number of databases for the environment
	enum maxdbs = DBs.length;
private:
	alias DBs = AliasSeq!(Meta, Tables, UniqueIndices!Tables);
	Env e;
	LMDB[maxdbs] dbs;

	enum indexOf(T) = staticIndexOf!(T, DBs);
	enum err = checkDBs!DBs;
	static assert(err == null, err);

	void buildIndex(T)() {
	}

	void vacuum() {
		foreach (i; 1 .. maxdbs) {
			LMDB db = dbs[i];
			if (db.txn) {
				db.txn.commit();
				db.txn = null;
			}
			if (db.dbi) {
				db.close();
				db.dbi = null;
			}
		}
	}

	void onUpdate(T)(ref T obj) {
		// TODO: check foreign key constraints
		// TODO: check unique constraints
		foreach (alias x; obj.tupleof) {
			static if (hasUDA!(x, nonEmpty)) {
				if (x == typeof(x).init)
					throw new Exception("Column " ~ x.stringof ~ " cannot be empty");
			}
		}
	}

	LMDB open(T)() {
		static assert(indexOf!T > -1, "Table " ~ T.stringof ~ " not found");
		LMDB db = dbs[indexOf!T];
		if (!db.txn)
			db.txn = env.begin();
		if (!db.dbi)
			db.dbi = db.txn.open(dbNameOf!T, DBFlags.create);
		return db;
	}

	LMDB open(T)(ref Txn txn) {
		static assert(indexOf!T > -1, "Table " ~ T.stringof ~ " not found");
		LMDB db = dbs[indexOf!T];
		if (!db.dbi)
			db.dbi = txn.open(dbNameOf!T, DBFlags.create);
		return db;
	}

	void store(T)(ref T obj) {
		alias filter = templateNot!isPK;

		Arr a;
		LMDB db = open!T();
		{
			mixin serialize!(obj, isPK);
			enum size = byteLen!(T, filter);
			static if (size) {
				a.m.mv_size = size;
			} else {
				a.m.mv_size = byteLen!(filter, intern, T)(obj);
			}

			int rc = db.set(bytes, a.v, WriteFlags.noOverwrite | WriteFlags.reserve);
			if (rc == MDB_KEYEXIST) {
				rc = db.set(bytes, a.v, WriteFlags.reserve);
			}
			check(rc);
		}
		{
			auto p = a.m.mv_data; // @suppress(dscanner.suspicious.unused_variable)
			mixin serialize!(obj, filter);
		}
	}

	void intern(T)(ref T[] data) @trusted {
		LMDB db = open!Meta();
		XXH64_hash_t seed;
	rehash:
		XXH64_hash_t[1] k = [xxh3_64Of(data, seed)];
		Val key = k[];
		Arr a = data;
		a.m.mv_size++;
		auto cursor = db.cursor();
		int rc = cursor.set(key, a.v, WriteFlags.noOverwrite | WriteFlags.reserve);
		if (rc == MDB_KEYEXIST) {
			Val val = a.v[0 .. data.length];
			if (likely(val == data)) {
				data = val;
				return;
			}
			seed = k[0];
			goto rehash;
		}
		check(rc);
		memcpy(a.m.mv_data, data.ptr, data.length);
		a.m.mark = 0;
		data = cast(T[])a.m.mv_data[0 .. data.length];
	}

	template serialize(alias obj, alias filter) {
		static if (is(typeof(p) == void)) {
			enum L = byteLen!(T, filter);
			static if (L) {
				enum keyLen = L;
				ubyte[L] buf = void;
				auto p = buf.ptr;
				// TODO: optimize for continuous key
			} else {
				const keyLen = byteLen!(filter, intern, T)(obj);
				auto p = cast(ubyte*)alloca(keyLen);
			}
		}
		auto bytes = {
			size_t i;
			foreach (ref x; obj.tupleof) {
				static if (filter!x) {
					alias U = typeof(x);
					static if (isArray!U) {
						static assert(!hasIndirections!(typeof(x[0])), "not implemented");
						static if (isDynamicArray!U) {
							if (x.length < lengthThreshold) { // inline
								// TODO: handle unaligned loads
								*cast(size_t*)(p + i) = x.length;
								i += size_t.sizeof;
								auto len = x.length * typeof(x[0]).sizeof;
								memcpy(p + i, x.ptr, len);
								i += len;
								continue;
							}
						}
					}
					*cast(Unqual!U*)(p + i) = x;
					i += U.sizeof;
				}
			}
			assert(i == keyLen);
			return p[0 .. i];
		}();
	}

public:
	@property ref env() => e;
	alias env this;

	this(size_t size) {
		e = create();
		e.mapsize = size;
		e.maxdbs = maxdbs;
	}

	~this() @trusted {
		close(env);
	}

	@disable this(this);

	void open(in char[] path, EnvFlags flags = EnvFlags.none, ushort mode = defaultMode) {
		auto cpath = cast(char*)alloca(path.length + 1);
		cpath[0 .. path.length] = path;
		cpath[path.length] = '\0';
		open(cpath, flags, mode);
	}

	/// ditto
	void open(scope const char* path, EnvFlags flags = EnvFlags.none, ushort mode = defaultMode) {
		if (!e) {
			e = create();
			e.maxdbs = Tables.length;
		}
		scope (failure)
			e.close();
		check(e.open(path, flags, mode));
	}

	void save(T)(in T obj) @trusted {
	}

	void del(T)(in T obj) {
		LMDB db = open!T();
		mixin serialize!(obj, isPK);
		check(db.del(bytes));
		// TODO: implement cascade delete
	}
}

struct Query {
private:
	//Txn txn;
	//MDB_dbi dbi;
public:
	ulong insertID;
	ulong affected;
}

version (unittest) {
	import std.stdio;

	@model
	struct User {
		@serial long id;
		@unique string name;
		@foreign!(Company.id) long companyID;
		long createdAt;
		long updatedAt;

		void onInsert(scope Next next) {
			createdAt = now();
			next();
		}

		void onSave(scope Next next) {
			updatedAt = now();
			next();
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
}

unittest {
	alias modules = AliasSeq!(lmdb_orm.orm);
	auto db = FSDB!modules(256 << 10);
	db.open("./test", EnvFlags.fixedMap | EnvFlags.noSubdir | EnvFlags.writeMap);
	writeln("maxreaders: ", db.maxreaders);
	writeln("maxkeysize: ", db.maxkeysize);
	writeln("flags: ", db.flags);
	writeln("envinfo: ", db.envinfo);
	writeln("stat: ", db.stat);
	check(db.sync());
	Txn txn = db.begin();
	writeln("id: ", txn.id);
	auto key = "foo";
	auto value = "3";
	check(db.set(key, value));
	txn.commit();
	db.txn = db.begin(TxnFlags.readOnly);
	writeln("stat: ", db.stat);
	foreach (k, v; db.cursor()) {
		writeln(cast(string)k, ": ", cast(string)v);
		writeln(k.ptr, ": ", v.ptr);
	}
}
