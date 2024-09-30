module lmdb_orm.orm;

import lmdb_orm.index;
import lmdb_orm.lmdb;
import lmdb_orm.oo;
import lmdb_orm.traits;
import std.meta;
import std.traits;

import core.stdc.stdlib;
import core.stdc.string;

debug import std.stdio;

/** Base class for exceptions thrown by the database. */
class DBException : Exception {
	import std.exception;

	mixin basicExceptionCtors;
}

private:
@model(blobDbName)
struct Blob;

template serialize(alias obj, alias filter, alias intern, bool alloc = true) {
	enum L = byteLen!(T, filter);
	static if (L) {
		enum length = L;
		ubyte[L] buf = void;
		static if (alloc)
			auto p = buf.ptr;
		// TODO: optimize for continuous key
	} else {
		const length = byteLen!(filter, intern)(obj);
		static if (alloc)
			auto p = cast(ubyte*)alloca(length);
	}
	auto bytes = {
		size_t i;
		alias T = typeof(obj);
		foreach (I, ref x; obj.tupleof) {
			static if (filter!(T.tupleof[I])) {
				alias U = typeof(x);
				alias O = OriginalType!U;
				static if (isArray!O) {
					static assert(!hasIndirections!(typeof(x[0])), "not implemented");
					static if (is(O == E[], E)) {
						static assert(!isMutable!E, "Element type of " ~ fullyQualifiedName!(
								T.tupleof[I]) ~ " must be immutable");
						if (x.length < lengthThreshold) { // inline
							assert(i + size_t.sizeof + x.length * E.sizeof <= length);
							// TODO: handle unaligned loads
							*cast(size_t*)(p + i) = x.length;
							i += size_t.sizeof;
							auto len = x.length * E.sizeof;
							memcpy(p + i, x.ptr, len);
							i += len;
							continue;
						}
						// TODO: handle ptr
					}
				}
				assert(i + U.sizeof <= length);
				*cast(Unqual!U*)(p + i) = x;
				i += U.sizeof;
			}
		}

		assert(i == length);
		return p[0 .. i];
	}();
}

T deserialize(T, P)(ref P pair) @trusted {
	T t;
	auto k = pair.key.ptr;
	auto v = pair.val.ptr;
	foreach (I, ref x; t.tupleof) {
		alias U = typeof(x);
		static if (isPK!(T.tupleof[I])) {
			alias p = k;
			const end = pair.key.ptr + pair.key.length;
		} else {
			alias p = v;
			const end = pair.val.ptr + pair.val.length;
		}
		static if (isArray!U) {
			static assert(!hasIndirections!(typeof(x[0])), "not implemented");
			static if (is(OriginalType!U == E[], E)) {
				static assert(!isMutable!E, "Element type of " ~ fullyQualifiedName!(
						T.tupleof[I]) ~ " must be immutable");
				assert(p + size_t.sizeof <= end);
				Arr a;
				const len = *cast(size_t*)p;
				p += size_t.sizeof;
				if (len < lengthThreshold) { // inline
					assert(p + len <= end);
					a.m.mv_size = len;
					a.m.mv_data = cast(void*)p;
					x = cast(U)a.v;
					p += len;
					continue;
				}
				// TODO: handle ptr
			}
		}
		assert(p + U.sizeof <= end);
		x = *cast(Unqual!U*)p;
		p += x.sizeof;
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

/// name of the blob database
enum blobDbName = "#blob";

/// Fixed-Schema Database
struct FSDB(modules...) {
	alias Tables = getTables!modules;
	/// Maximum number of databases for the environment
	enum maxdbs = DBs.length;

	struct Txn {
		private MDB_txn* txn;

		alias handle this;
		@property auto handle() => txn;

		@property {
			ref db() @trusted
				=> *cast(FSDB!modules*)(txn.env.userctx);
			/// Get the database flags.
			DBFlags flags(T)() @trusted {
				uint flags = void;
				check(mdb_dbi_flags(txn, open!T(), &flags));
				return cast(DBFlags)flags;
			}

			/// Get the database statistics.
			Stat stat(T)() @trusted {
				Stat info = void;
				check(mdb_stat(txn, open!T(), cast(MDB_stat*)&info));
				return info;
			}
		}

		private this(MDB_txn* t, ref FSDB!modules db)
		in (t !is null) {
			txn = t;
			t.env.userctx = &db;
		}

		~this() @trusted {
			//abort(txn);
		}

		//private alias close = abort;

		@disable this(this);
	private:
		void buildIndex(T)() {
		}

		void store(T)(ref T obj) @trusted {
			alias filter = templateNot!isPK;

			const dbi = open!T();
			scope dg = &intern;
			mixin getSerial!T;
			static if (getSerial != serial(0, 0, 0)) {
				alias t = obj.tupleof;
				if (!t[index]) {
					auto cur = cursor!T(CursorOp.last);
					t[index] = cur.empty ? 1 : *cast(typeof(t[index])*)cur.key.ptr + 1;
				}
			}
			Arr a;
			{
				mixin serialize!(obj, isPK, dg);
				enum size = byteLen!(T, filter);
				static if (size) {
					a.m.mv_size = size;
				} else {
					a.m.mv_size = byteLen!(filter, dg, T)(obj);
				}

				debug writeln("save key: ", bytes);
				auto key = cast(MDB_val*)&bytes;
				int rc = mdb_put(txn, dbi, key, &a.m,
					WriteFlags.noOverwrite | WriteFlags.reserve);
				if (rc == MDB_KEYEXIST) {
					rc = mdb_put(txn, dbi, key, &a.m, WriteFlags.reserve);
					check(rc);
				} else {
					check(rc);
					static if (__traits(getOverloads, obj, "onSave").length)
						obj.onSave(null);
				}
			}
			{
				auto p = a.m.mv_data; // @suppress(dscanner.suspicious.unused_variable)
				mixin serialize!(obj, filter, dg, false);
				debug writeln("save val: ", bytes);
			}
		}

		void intern(ref Val data) @trusted {
			import lmdb_orm.xxh3;

			const dbi = open!Blob();
			XXH64_hash_t seed;
		rehash:
			XXH64_hash_t[1] k = [xxh3_64Of(data, seed)];
			Val key = k[];
			Arr a = Arr(data);
			a.m.mv_size++;
			int rc = mdb_put(txn, dbi, cast(MDB_val*)&key, &a.m,
				WriteFlags.noOverwrite | WriteFlags.reserve);
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
			data = a.m.mv_data[0 .. data.length];
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

		MDB_dbi open(T)() @trusted {
			alias U = Unqual!T;
			static assert(indexOf!U >= 0, "Table " ~ U.stringof ~ " not found");
			enum flags = DBFlags.create |
				(getSerial!U == serial(0, 0, 0) ? 0 : DBFlags.integerKey);
			auto dbi = &db.dbs[indexOf!U];
			if (!*dbi)
				check(mdb_dbi_open(txn, dbNameOf!U, flags, dbi));
			return *dbi;
		}

	public:

		/** Begin a transaction within the current transaction.
		Params:
		flags = optional transaction flags.
		Returns: a transaction handle
		*/
		Txn begin(TxnFlags flags = TxnFlags.none) @trusted {
			MDB_txn* t = void;
			check(mdb_txn_begin(txn.env, txn, flags, &t));
			return Txn(t, db);
		}

		/** Commit a transaction. */
		void commit() @trusted {
			check(mdb_txn_commit(txn));
			txn = null;
		}

		auto cursor(T)(CursorOp op = CursorOp.next) {
			MDB_cursor* cur = void;
			check(mdb_cursor_open(txn, open!T(), &cur));
			return Cursor!T(cur, op);
		}

		void save(T)(T obj) @trusted {
			scope Next next = () @trusted { onUpdate(obj); store(obj); };
			static if (__traits(getOverloads, obj, "onSave").length) {
				//static assert(__traits(getParameterStorageClasses, foo, 0)[0] == "scope");
				obj.onSave(next);
			} else {
				next();
			}
		}

		bool del(T)(in T obj) {
			const dbi = open!T();
			mixin serialize!(obj, isPK, intern);
			int rc = mdb_del(txn, dbi, cast(MDB_val*)&bytes, null);
			if (rc == MDB_NOTFOUND)
				return false;
			check(rc);
			// TODO: implement cascade delete
			return true;
		}
	}

private:
	alias DBs = AliasSeq!(Blob, Tables, UniqueIndices!Tables);
	Env e;
	MDB_dbi[maxdbs] dbs;

	enum indexOf(T) = staticIndexOf!(T, DBs);
	enum err = checkDBs!DBs;
	static assert(err == null, err);

public:
	@property ref env() => e;
	alias env this;

	void* userctx;

	this(size_t size) {
		e = create();
		e.mapsize = size;
		e.maxdbs = maxdbs;
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
			e.maxdbs = maxdbs;
		}
		check(e.open(path, flags, mode));
	}

	/** Begin a transaction.
	Params:
	env = the environment handle
	flags = optional transaction flags.
	parent = handle of a transaction that may be a parent of the new transaction.
	Returns: a transaction handle
	*/
	Txn begin(TxnFlags flags = TxnFlags.none, MDB_txn* parent = null) @trusted {
		MDB_txn* txn = void;
		check(mdb_txn_begin(env, parent, flags, &txn));
		return Txn(txn, this);
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
	auto dbi()
		=> mdb_cursor_dbi(cursor);

	bool empty() @trusted {
		if (opOffset <= rc && rc <= opOffset + CursorOp.max)
			rc = mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, cast(MDB_cursor_op)(
					rc - opOffset));
		if (rc != MDB_NOTFOUND)
			check(rc);
		return rc == MDB_NOTFOUND;
	}

	T front() @safe {
		return deserialize!T(this);
	}

	alias back = front;
}

unittest {
	import std.meta;
	import std.stdio;

	alias modules = AliasSeq!(lmdb_orm.traits);
	auto db = FSDB!modules(256 << 10);
	db.open("./test2", EnvFlags.writeMap);
	auto txn = db.begin();
	txn.save(User(0, "Alice", 0));
	txn.save(User(0, "Bob", 1));
	txn.commit();
	txn = db.begin(TxnFlags.readOnly);
	foreach (user; txn.cursor!User()) {
		writeln(user);
	}
	txn = db.begin();
	writeln(txn.id);
	txn.del(User(1));
	txn.commit();
}
