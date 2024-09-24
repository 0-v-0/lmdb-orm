module lmdb_orm.orm;

import lmdb_orm.oo;
import lmdb_orm.traits;
import std.meta;
import std.traits;

import core.stdc.stdlib;

alias Next = void delegate();

@model(metaDbName)
private struct Meta;

private enum lengthThreshold = 64;

private size_t valueSize(alias filter, alias store, T)(in T value) {
	size_t size;
	foreach (const ref x; value.tupleof) {
		static if (filter!x) {
			static if (isArray!(typeof(x))) {
				static if (hasIndirections!(typeof(x[0]))) {
					foreach (const ref y; x)
						size += valueSize!filter(y);
				} else {
					static if (isStaticArray!(typeof(x))) {
						size += x.length * typeof(x[0]).sizeof;
					} else {
						if (x.length < lengthThreshold) // inline
							size += size_t.sizeof + x.length * typeof(x[0]).sizeof;
						else {
							store(x);
							size += Val.sizeof;
						}
					}
				}
			} else
				size += x.sizeof;
		}
	}
	return size;
}

/// name of the meta database
enum metaDbName = "#meta";
//static assert(__traits(getParameterStorageClasses, foo, 0)[0] == "scope");

/// Fixed-Schema Database
struct FSDB(modules...) {
	alias Tables = getTables!modules;
	/// Maximum number of databases for the environment
	enum maxdbs = _Tables.length;
private:
	alias _Tables = AliasSeq!(Meta, Tables);
	Env e;
	LMDB[maxdbs] dbs;

	enum indexOf(T) = staticIndexOf!(T, _Tables);
	enum err = verifyTables();
	static assert(err == null, err);

	string verifyTables() {
		if (__ctfe) {
			size_t[string] indices;
			foreach (i, T; _Tables) {
				auto name = dbNameOf!T;
				if (name in indices)
					return "Table " ~ T.stringof ~ " has the same name as " ~
						_Tables[indices[name]].stringof;
				indices[name] = i;
			}
		}
		return null;
	}

	void buildIndex(T)() {

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

	void save(T)(in T obj) {
		LMDB db = open!T();
		check(db.set(obj));
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
