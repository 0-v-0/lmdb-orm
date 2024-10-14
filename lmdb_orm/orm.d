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
		struct Tuple(T...) {
			T expand;
		}

		size_t[string] indices;
		foreach (i, T; DBs) {
			enum name = modelOf!T.name;
			if (name in indices)
				return "Table " ~ T.stringof ~ " has the same name as " ~
					[staticMap!(nameOf, DBs)][indices[name]];
			alias E = Tuple!(typeof(T.tupleof)).tupleof;
			foreach (j, alias f; T.tupleof) {
				if (f.offsetof != E[i].offsetof)
					return "Field " ~ fullyQualifiedName!f ~ " of " ~ T.stringof ~ " is out of order";
			}
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

pragma(inline, true) static void checkEmpty(alias f)(ref typeof(f) val) {
	static if (hasUDA!(f, nonEmpty))
		if (val == typeof(f).init)
			throw new DBException("Column " ~ fullyQualifiedName!f ~ " cannot be empty");
}

/// name of the blob database
enum blobDbName = "#blob";

/// Fixed-Schema Database
public struct FSDB(modules...) {
	alias Tables = getTables!modules;
	/// Maximum number of databases for the environment
	enum maxdbs = DBs.length;

	/// A transaction handle.
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
			abort(txn);
		}

		//private alias close = abort;

		@disable this(this);
	private:
		void buildIndex(T)() {
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
			enum flags = DBFlags.create | modelOf!U.flags |
				(getSerial!U == serial.invalid ? DBFlags.none : DBFlags.integerKey);
			auto dbi = &db.dbs[indexOf!U];
			pragma(msg, "open " ~ modelOf!U.name);
			if (!*dbi)
				check(mdb_dbi_open(txn, modelOf!U.name, flags, dbi));
			return *dbi;
		}

		void store(T)(MDB_dbi dbi, ref T obj) @trusted {
			alias filter = templateNot!isPK;

			mixin getSerial!T;
			auto flags = WriteFlags.reserve;
			static if (getSerial != serial.invalid) {
				alias t = obj.tupleof;
				if (!t[index]) {
					auto cur = Cursor!T(txn, dbi, CursorOp.last, checkFlags);
					alias S = typeof(t[index]);
					S id = cur.empty ? cast(S)getSerial.min
						: *cast(S*)cur.key.ptr + cast(S)getSerial.step;
					if (id > cast(S)getSerial.max)
						throw new DBException("Serial overflow");
					if (id < cast(S)getSerial.min)
						throw new DBException("Serial underflow");
					t[index] = id;
					flags |= WriteFlags.append;
				}
			}
			onUpdate(dbi, obj);
			scope dg = &intern;
			Arr a;
			{
				mixin serialize!(obj, isPK, dg);
				enum size = byteLen!(T, filter);
				static if (size) {
					a.m.mv_size = size;
				} else {
					a.m.mv_size = byteLen!(filter, dg, T)(obj);
				}

				auto key = cast(MDB_val*)&bytes;
				int rc = mdb_put(txn, dbi, key, &a.m,
					WriteFlags.noOverwrite | flags);
				if (rc == MDB_KEYEXIST) {
					rc = mdb_put(txn, dbi, key, &a.m, flags);
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
			}
		}

		void onUpdate(T)(MDB_dbi dbi, ref T obj) @trusted {
			if (checkFlags & CheckFlags.empty) {
				foreach (i, ref x; obj.tupleof)
					checkEmpty!(T.tupleof[i])(x);
			}
			// TODO: check foreign key constraints
			if (checkFlags & CheckFlags.unique) {
				scope dg = &intern;
				foreach (i, ref x; obj.tupleof) {
					static if (hasUDA!(T.tupleof[i], unique)) {
						enum filter(alias x) = __traits(isSame, x, T.tupleof[i]);
						mixin serialize!(obj, filter, dg);
						Arr a;
						{
							enum size = byteLen!(T, isPK);
							static if (size) {
								a.m.mv_size = size;
							} else {
								a.m.mv_size = byteLen!(isPK, dg, T)(obj);
							}
						}
						const rc = mdb_put(txn, open!(UniqueIndex!(T, T.tupleof[i])), cast(MDB_val*)&bytes,
							cast(MDB_val*)&a.m,
							WriteFlags.noOverwrite | WriteFlags.reserve);
						if (rc == MDB_KEYEXIST)
							throw new DBException("Column " ~ fullyQualifiedName!(
									T.tupleof[i]) ~ " must be unique");
						{
							auto p = a.m.mv_data; // @suppress(dscanner.suspicious.unused_variable)
							mixin serialize!(obj, isPK, dg, false);
						}
					}
				}
			}
			if (checkFlags & CheckFlags.foreign)
				foreach (i, ref x; obj.tupleof) {
					foreach (a; __traits(getAttributes, T.tupleof[i])) {
						static if (is(a : foreign!f, alias f))
							if (!exists!(__traits(parent, f), __traits(identifier, f))(x))
								throw new DBException("Foreign key " ~ fullyQualifiedName!(
										T.tupleof[i]) ~ " not found");
					}
				}
		}

	public:
		/// Check flags for column.
		CheckFlags checkFlags = CheckFlags.all;

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

		auto cursor(T)(CursorOp op = CursorOp.next)
			=> Cursor!T(txn, open!T(), op, checkFlags);

		void save(T)(T obj) @trusted {
			scope Next next = () @trusted {
				const dbi = open!T();
				store(dbi, obj);
			};
			static if (is(typeof(obj.onSave(next)))) {
				static foreach (sc; __traits(getParameterStorageClasses, obj.onSave(next), 0)) {
					static if (sc == "scope")
						enum isScope = true;
				}
				static assert(is(typeof(isScope)), "The first parameter of " ~ fullyQualifiedName!(
						obj.onSave) ~ " must be scope");
				obj.onSave(next);
			} else {
				next();
			}
		}

		template exists(T) {
			alias x = Filter!(isPK, T.tupleof);
			bool exists(typeof(x) obj) @trusted {
				const dbi = open!T();
				Val val = void;
				mixin serialize!(obj, isPK, intern);
				const rc = mdb_get(txn, dbi, cast(MDB_val*)&key, cast(MDB_val*)&val);
				if (rc == MDB_NOTFOUND)
					return false;
				check(rc);
				return true;
			}
		}

		template exists(T, string member) {
			alias x = __traits(getMember, T, member);
			bool exists(typeof(x) key) @trusted {
				static if (isPK!x) {
					// TODO: check compound primary key
					const dbi = open!T();
				} else {
					const dbi = open!(UniqueIndex!(T, x))();
				}
				Val val = void;
				const rc = mdb_get(txn, dbi, cast(MDB_val*)&key, cast(MDB_val*)&val);
				if (rc == MDB_NOTFOUND)
					return false;
				check(rc);
				return true;
			}
		}

		bool del(T)(in T obj) @trusted {
			const dbi = open!T();
			mixin serialize!(obj, isPK, intern);
			const rc = mdb_del(txn, dbi, cast(MDB_val*)&bytes, null);
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
	package MDB_cursor* cursor;
	alias cursor this;
	Val key;
	Val val;
	private int rc;
	CheckFlags checkFlags = CheckFlags.all;

	private this(MDB_txn* txn, MDB_dbi dbi, CursorOp op, CheckFlags flags) @trusted {
		check(mdb_cursor_open(txn, dbi, &cursor));
		rc = cast(int)op + opOffset;
		checkFlags = flags;
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
	void save(WriteFlags flags = WriteFlags.none)
		=> check(mdb_cursor_put(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, flags));

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

	auto db = DB(256 << 10);
	db.open("./db/test2", EnvFlags.writeMap);
	auto txn = db.begin();
	txn.save(User(0, "Alice", 0));
	txn.save(User(0, "Bob", 1));
	txn.commit();
	txn = db.begin(TxnFlags.readOnly);
	foreach (user; txn.cursor!User()) {
		writeln(user);
	}
	txn = db.begin();
	foreach (user; txn.cursor!User()) {
		user.companyID = 1;
	}
	txn.commit();
	txn = db.begin();
	writeln(txn.id);
	txn.del(User(1));
	txn.commit();
}
