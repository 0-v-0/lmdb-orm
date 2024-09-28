module lmdb_orm.orm;

import lmdb_orm.lmdb;
import lmdb_orm.oo;
import lmdb_orm.traits;
import std.meta;
import std.traits;

import core.stdc.stdlib;
import core.stdc.string;

/** Base class for exceptions thrown by the database. */
class DBException : Exception {
	import std.exception;

	mixin basicExceptionCtors;
}

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

template serialize(alias obj, alias filter, alias intern) {
	static if (is(typeof(p) == void)) {
		enum L = byteLen!(T, filter);
		static if (L) {
			enum keyLen = L;
			ubyte[L] buf = void;
			auto p = buf.ptr;
			// TODO: optimize for continuous key
		} else {
			const keyLen = byteLen!(filter, intern)(obj);
			auto p = cast(ubyte*)alloca(keyLen);
		}
	}
	auto bytes = {
		size_t i;
		foreach (ref x; obj.tupleof) {
			static if (filter!x) {
				alias U = typeof(x);
				alias O = OriginalType!U;
				static if (isArray!O) {
					static assert(!hasIndirections!(typeof(x[0])), "not implemented");
					static if (is(O == E[], E)) {
						static assert(!isMutable!E, "Element type of " ~ x.stringof ~ " must be immutable");
						if (x.length < lengthThreshold) { // inline
							assert(i + size_t.sizeof + x.length * E.sizeof <= keyLen);
							// TODO: handle unaligned loads
							*cast(size_t*)(p + i) = x.length;
							i += size_t.sizeof;
							auto len = x.length * E.sizeof;
							memcpy(p + i, x.ptr, len);
							i += len;
							continue;
						}
					}
				}
				assert(i + U.sizeof <= keyLen);
				*cast(Unqual!U*)(p + i) = x;
				i += U.sizeof;
			}
		}
		assert(i == keyLen);
		return p[0 .. i];
	}();
}

T deserialize(T)(Val key, Val val) @trusted {
	T t;
	auto k = key.ptr;
	auto v = val.ptr;
	foreach (ref x; t.tupleof) {
		alias U = typeof(x);
		static if (isPK!x) {
			alias p = k;
			alias buf = key;
		} else {
			alias p = v;
			alias buf = val;
		}
		const end = buf.ptr + buf.length;
		static if (isArray!U) {
			static assert(!hasIndirections!(typeof(x[0])), "not implemented");
			static if (is(O == E[], E)) {
				static assert(!isMutable!E, "Element type of " ~ x.stringof ~ " must be immutable");
				assert(p + size_t.sizeof <= end);
				Arr a;
				auto len = *cast(size_t*)p;
				p += size_t.sizeof;
				if (len < lengthThreshold) { // inline
					assert(p + len <= end);
					a.m.mv_size = len;
					a.m.mv_data = p;
					x = cast(U)a.v;
					continue;
				}
				// TODO: handle ptr
			}
		}
		assert(p + U.sizeof <= end);
		x = *cast(Unqual!U*)p;
		p += x.sizeof;
		if (p > k.ptr + len)
			break;
	}
	return t;
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

void onUpdate(T)(ref T obj) {
	// TODO: check foreign key constraints
	// TODO: check unique constraints
	foreach (alias x; obj.tupleof) {
		static if (hasUDA!(x, nonEmpty)) {
			if (x == typeof(x).init)
				throw new DBException("Column " ~ x.stringof ~ " cannot be empty");
		}
	}
}

union Arr {
	Val v;
	MDB_val m;
}

@property ref mark(MDB_val m)
	=> (cast(ubyte*)m.mv_data)[m.mv_size - 1];

/// name of the meta database
enum metaDbName = "#meta";

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

	/+ TODO
	void vacuum() {
		static foreach (i; 1 .. maxdbs) {
			foreach(val; cursor!(DBs[i])) {
				dbs[0].set(val.key, val.val);
			}
		}
		db[0].txn.commit();
	}+/

	LMDB open(T)() {
		static assert(indexOf!T > -1, "Table " ~ T.stringof ~ " not found");
		LMDB db = dbs[indexOf!T];
		if (!db.txn)
			db.txn = env.begin();
		return open!T(db);
	}

	LMDB open(T)(LMDB db) {
		enum flags = DBFlags.create |
			(getSerial!T == serial(0, 0, 0) ? 0 : DBFlags.integerKey);
		if (!db.dbi)
			db.dbi = db.txn.open(dbNameOf!T, flags);
		return db;
	}

	void store(T)(ref T obj) {
		alias filter = templateNot!isPK;

		Arr a;
		LMDB db = open!T();
		{
			mixin serialize!(obj, isPK, intern);
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
			mixin serialize!(obj, filter, intern);
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
	/+
	void open(in char[] path, EnvFlags flags = EnvFlags.none, ushort mode = defaultMode) {
		auto cpath = cast(char*)alloca(path.length + 1);
		cpath[0 .. path.length] = path;
		cpath[path.length] = '\0';
		open(cpath, flags, mode);
	}
	+/

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

	auto cursor(T)(CursorOp op = CursorOp.next)
		=> open!T().cursor(op);

	void save(T)(in T obj) @trusted {
		scope Next next = () { onUpdate(obj); store(obj); };
		static if (__traits(getOverloads, obj, "onSave").length) {
			//static assert(__traits(getParameterStorageClasses, foo, 0)[0] == "scope");
			obj.onSave(next);
		} else {
			next();
		}
	}

	void del(T)(in T obj) {
		LMDB db = open!T();
		mixin serialize!(obj, isPK, intern);
		check(db.del(bytes));
		// TODO: implement cascade delete
	}
}

struct Cursor(T) {
	private MDB_cursor* cursor;
	alias cursor this;
	Val key;
	Val val;
	private int rc;
	private this(MDB_cursor* cur, CursorOp op)
	in (cur !is null) {
		cursor = cur;
		rc = cast(int)op + opOffset;
	}

	~this() @trusted {
		close(cursor);
	}

	@disable this(this);

	void popFront() @trusted {
		rc = mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, CursorOp.next);
	}

	void popBack() @trusted {
		rc = mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, CursorOp.prev);
	}

	/// Store by cursor.
	void save(T, U)(in T[] key, ref U[] val, WriteFlags flags = WriteFlags.none)
		=> mdb_cursor_put(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, flags);

	/// Delete by cursor.
	void del(DeleteFlags flags = DeleteFlags.none)
		=> check(mdb_cursor_del(cursor, flags));

@property:
	/// Return count of duplicates for current key.
	size_t count() @trusted {
		size_t count = void;
		check(mdb_cursor_count(cursor, &count));
		return count;
	}

	/// Return the cursor's database handle.
	auto dbi() @trusted
		=> mdb_cursor_dbi(cursor);

	bool empty() @trusted {
		if (opOffset <= rc && rc <= opOffset + CursorOp.max)
			rc = mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, cast(MDB_cursor_op)(
					rc - opOffset));
		if (rc != MDB_NOTFOUND)
			check(rc);
		return rc == MDB_NOTFOUND;
	}

	T front() @trusted {
		return deserialize!T(key, val);
	}

	alias back = front;
}

unittest {
	import std.meta;
	import std.stdio;

	alias modules = AliasSeq!(lmdb_orm.traits);
	auto db = FSDB!modules(256 << 10);
	db.open("./test", EnvFlags.fixedMap | EnvFlags.noSubdir | EnvFlags.writeMap);
	Txn txn = db.begin();
	txn.save(User(1, "John Doe", 1, 1));
	foreach (user; db.cursor()) {
		writeln(user);
	}
}
