module lmdb_orm.orm;

import lmdb_orm.lmdb : MDB_val, MDB_KEYEXIST;
import lmdb_orm.oo;
import lmdb_orm.traits;
import std.meta;
import std.traits;

import core.stdc.stdlib;
import core.stdc.string;

alias Next = void delegate();

@model(metaDbName)
private struct Meta;

/// name of the meta database
enum metaDbName = "#meta";
//static assert(__traits(getParameterStorageClasses, foo, 0)[0] == "scope");

/// Fixed-Schema Database
struct FSDB(modules...) {
	alias Tables = getTables!modules;
	/// Maximum number of databases for the environment
	enum maxdbs = DBs.length;
private:
	alias DBs = AliasSeq!(Meta, Tables);
	Env e;
	LMDB[maxdbs] dbs;

	enum indexOf(T) = staticIndexOf!(T, DBs);
	enum err = verifyTables();
	static assert(err == null, err);

	string verifyTables() {
		if (__ctfe) {
			size_t[string] indices;
			foreach (i, T; DBs) {
				auto name = dbNameOf!T;
				if (name in indices)
					return "Table " ~ T.stringof ~ " has the same name as " ~
						DBs[indices[name]].stringof;
				indices[name] = i;
			}
		}
		return null;
	}

	void buildIndex(T)() {

	}

	void vacuum() {
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

	void store(T)(in T obj) {
		LMDB db = open!Meta();
		check(db.set(obj));
	}

	void intern(T)(ref T[] data) {
		LMDB db = open!Meta();
		XXH64_hash_t seed;
	rehash:
		XXH64_hash_t[1] k = [xxh3_64Of(data, seed)];
		Val key = k[];
		Val val = data;
		auto cursor = db.cursor();
		int rc = cursor.set(key, val, WriteFlags.noOverwrite);
		if (rc == MDB_KEYEXIST) {
			if (likely(val == data)) {
				data = val;
				return;
			}
			seed = k[0];
			goto rehash;
		}
		check(rc);
		check(cursor.get(key, data, CursorOp.getCurrent));
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
	@property auto env() => e;
	alias env this;

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
		union U {
			Val v;
			MDB_val m;
		}
		alias filter = templateNot!isPK;

		U u;
		LMDB db = open!T();
		{
			mixin serialize!(obj, isPK);
			enum size = byteLen!(T, filter);
			static if (size) {
				u.m.mv_size = size;
			} else {
				u.m.mv_size = byteLen!(filter, intern, T)(obj);
			}

			int rc = db.set(bytes, u.v, WriteFlags.noOverwrite | WriteFlags.reserve);
			if (rc == MDB_KEYEXIST) {
				rc = db.set(bytes, u.v, WriteFlags.reserve);
			}
			check(rc);
		}
		{
			auto p = u.m.mv_data; // @suppress(dscanner.suspicious.unused_variable)
			mixin serialize!(obj, filter);
		}
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

version (none) unittest {
	alias modules = AliasSeq!(lmdb_orm.orm);
	FSDB!modules db;
	env.mapsize = 256 << 10;
	env.maxdbs = 2;
	db.open("./test", EnvFlags.fixedMap | EnvFlags.noSubdir | EnvFlags.writeMap);
	writeln("maxreaders: ", db.maxreaders);
	writeln("maxkeysize: ", db.maxkeysize);
	writeln("flags: ", db.flags);
	writeln("envinfo: ", db.envinfo);
	writeln("stat: ", db.stat);
	check(db.sync());
	Txn txn = env.begin();
	writeln("id: ", txn.id);
	LMDB db = txn.open("test", DBFlags.create);
	scope (exit)
		db.txn.abort();
	scope (exit)
		db.close();
	auto key = "foo";
	auto value = "3";
	check(db.set(key, value));
	txn.commit();
	db.txn = env.begin(TxnFlags.readOnly);
	writeln("stat: ", db.stat);
	foreach (k, v; db.cursor()) {
		writeln(cast(string)k, ": ", cast(string)v);
		writeln(k.ptr, ": ", v.ptr);
	}
}
