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

pragma(inline, true) package static void checkEmpty(alias f)(ref typeof(f) val) {
	static if (hasUDA!(f, nonEmpty))
		if (val == typeof(f).init)
			throw new DBException("Column " ~ fullyQualifiedName!f ~ " cannot be empty");
}

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
				check(mdb_dbi_flags(txn, openDB!T(txn), &flags));
				return cast(DBFlags)flags;
			}

			/// Get the database statistics.
			Stat stat(T)() @trusted {
				Stat info = void;
				check(mdb_stat(txn, openDB!T(txn), cast(MDB_stat*)&info));
				return info;
			}
		}

		private this(MDB_txn* t, ref FSDB!modules db)
		in (t !is null) {
			txn = t;
			t.env.userctx = &db;
		}
		// dfmt off
		~this() @trusted {
			.abort(txn);
		}

		@disable this(this);
// dfmt on

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

		/** Abort a transaction. */
		void abort() @trusted {
			mdb_txn_abort(txn);
			txn = null;
		}

		/** Commit a transaction. */
		void commit() @trusted {
			check(mdb_txn_commit(txn));
			txn = null;
		}

		auto cursor(T)(CursorOp op = CursorOp.next)
			=> Cursor!T(txn, openDB!T(txn), op, checkFlags);

		auto mapper(T)(CursorOp op = CursorOp.next)
			=> Cursor!(T, true)(txn, openDB!T(txn), op, checkFlags);

		void save(T)(T obj) @trusted {
			scope Next next = { store(obj); };
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

	private:
		void buildIndex(T)() {
		}

		/+ TODO
		void vacuum() {
			static foreach (i; 1 .. maxdbs) {
				foreach(val; cursor!(DBs[i])) {
					openDB!Blob(txn).set(val.key, val.val);
				}
			}
			db[0].txn.commit();
		}+/

		void store(T)(ref T obj) @trusted {
			mixin getSerial!T;
			const dbi = openDB!T(txn);
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
			onUpdate(txn, checkFlags, obj);
			Arr a;
			{
				mixin serialize!(obj, 0, keyCount!T);
				setSize(a, obj, txn, &intern);
				int rc = mdb_put(txn, dbi, &bytes, &a.m,
					WriteFlags.noOverwrite | flags);
				if (rc == MDB_KEYEXIST) {
					rc = mdb_put(txn, dbi, &bytes, &a.m, flags);
					check(rc);
				} else {
					check(rc);
					static if (is(typeof(obj.onSave(next))))
						obj.onSave(null);
				}
			}
			auto p = a.m.mv_data; // @suppress(dscanner.suspicious.unused_variable)
			mixin serialize!(obj, keyCount!T);
		}
	}

private:
	alias DBs = AliasSeq!(Blob, Tables, UniqueIndices!Tables);
	Env e;

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

template exists(T, string member = null) {
	static if (member) {
		alias x = __traits(getMember, T, member);
		bool exists(MDB_txn* txn, typeof(x) key) @trusted {
			mixin tryGet!key;
			if (rc == MDB_NOTFOUND)
				return false;
			check(rc);
			return true;
		}
	} else {
		bool exists(MDB_txn* txn, TKey!T key) @trusted {
			alias x = Alias!(T.tupleof[0]);
			mixin tryGet!key;
			if (rc == MDB_NOTFOUND)
				return false;
			check(rc);
			return true;
		}
	}
}

template findBy(T, string member) {
	alias x = __traits(getMember, T, member);
	T findBy(MDB_txn* txn, typeof(x) key) @trusted {
		mixin tryGet!key;
		check(rc);
		return deserialize!T(bytes, val);
	}

	T findBy(MDB_txn* txn, typeof(x) key, T defValue) @trusted {
		mixin tryGet!key;
		if (rc == MDB_NOTFOUND)
			return defValue;
		check(rc);
		return deserialize!T(bytes, val);
	}
}

bool del(T)(MDB_txn* txn, TKey!T key) @trusted {
	const dbi = openDB!T(txn);
	auto obj = Tuple!(typeof(key))(key);
	mixin serialize!(obj, 0);
	const rc = mdb_del(txn, dbi, &bytes, null);
	if (rc == MDB_NOTFOUND)
		return false;
	check(rc);
	// TODO: implement cascade delete
	return true;
}

struct Cursor(T, bool proxy = false) {
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

	static if (proxy) {
		import lmdb_orm.proxy;

		auto front() => Proxy!T(&this);
	} else {
		T front() @safe => deserialize!T(key, val);
	}

	alias back = front;
}

private:
/// name of the blob database
enum blobDbName = "#blob";

@model(blobDbName, DBFlags.integerKey)
struct Blob;

template serialize(alias obj, size_t start, size_t end = obj.tupleof.length, bool setIntern = true) {
	import core.volatile;
	import std.file;

	alias TT = typeof(obj.tupleof[start .. end]);
	alias P = Tuple!(typeof(TT));
	static if (isFixedSize!TT) {
		static if (is(typeof(p + 1)))
			auto _ = *cast(P*)p = P(obj.tupleof[start .. end]);
		else
			auto bytes = MDB_val(P.sizeof, cast(void*)&obj);
	} else {
		static if (setIntern)
			const length = byteLen!(start, end)(obj, txn, &intern);
		else
			const length = byteLen!(start, end)(obj, txn);
		static if (!is(typeof(p + 1)))
			auto p = alloca(length);
		auto bytes = {
			assert(P.sizeof <= length);
			size_t i = P.sizeof;
			foreach (I, ref x; obj.tupleof[start .. end]) {
				alias U = typeof(x);
				alias O = OriginalType!U;
				enum offset = P.tupleof[I].offsetof;
				static if (isArray!O) {
					static assert(!hasIndirections!(typeof(x[0])), "not implemented");
					static if (is(O == E[], E)) {
						static assert(!isMutable!E, "Element type of " ~ fullyQualifiedName!(
								args[I]) ~ " must be immutable");
						if (x.length < lengthThreshold) { // inline
							const len = x.length * E.sizeof;
							assert(i + len <= length);
							memcpy(p + i, x.ptr, len);
							*cast(size_t[2]*)(p + offset) = [x.length, i];
							i += len;
							continue;
						}
					}
				}
				*cast(Unqual!U*)(p + offset) = x;
			}

			assert(i == length);
			return Arr(p[0 .. i]).m;
		}();
	}
}

T deserialize(T)(MDB_val val) @trusted {
	if (val.mv_size == T.sizeof) {
		return *cast(T*)val.mv_data;
	}
	assert(0, "unimplemented");
}

T deserialize(T)(Val key, Val val) @trusted {
	enum _keyCount = keyCount!T;
	alias K = Tuple!(TKey!T);
	alias V = Tuple!(typeof(T.tupleof[_keyCount .. $]));
	if (key.length < K.sizeof)
		throw new DBException("Key length mismatch");
	if (val.length < V.sizeof) {
		debug writeln("val ", val);
		throw new DBException("Value length mismatch");
	}
	T t;
	//if (key.length == K.sizeof) {
	//	*cast(K*)&t = *cast(K*)key.ptr;
	//}
	//if (val.length == V.sizeof) {
	//	*cast(V*)&t = *cast(V*)val.ptr;
	//}
	const k = key.ptr;
	const v = val.ptr;
	foreach (I, ref x; t.tupleof) {
		alias U = typeof(x);
		static if (I < _keyCount) {
			alias p = k;
			const length = key.length;
			enum offset = K.tupleof[I].offsetof;
		} else {
			alias p = v;
			const length = val.length;
			enum offset = V.tupleof[I - _keyCount].offsetof;
		}
		static if (isArray!U) {
			static assert(!hasIndirections!(typeof(x[0])), "not implemented");
			static if (is(OriginalType!U == E[], E)) {
				static assert(!isMutable!E, "Element type of " ~ fullyQualifiedName!(
						T.tupleof[I]) ~ " must be immutable");
				assert(offset + Val.sizeof <= length);
				Arr a = *cast(Arr*)(p + offset);
				if (a.v.length < lengthThreshold) { // inline
					assert(a.v.length + a.s[1] <= length);
					a.m.mv_data += cast(size_t)p;
					x = cast(U)a.v;
					continue;
				}
				// TODO: handle ptr
			}
		}
		assert(offset + U.sizeof <= length);
		x = *cast(Unqual!U*)(p + offset);
	}
	return t;
}

string checkDBs(DBs...)() {
	if (__ctfe) {
		enum nameOf(T) = T.stringof;

		size_t[string] indices;
		foreach (i, T; DBs) {
			enum name = modelOf!T.name;
			if (name in indices)
				return "Table " ~ T.stringof ~ " has the same name as " ~
					[staticMap!(nameOf, DBs)][indices[name]];
			alias E = Tuple!(typeof(T.tupleof)).tupleof;
			foreach (j, alias f; T.tupleof) {
				static assert(!isPointer!(typeof(f)) && !isDelegate!f,
					"Field " ~ fullyQualifiedName!f ~ " of " ~ T.stringof ~ " is a pointer");
				if (f.offsetof != E[j].offsetof)
					return "Field " ~ f.stringof ~ " in " ~ fullyQualifiedName!T ~ " is misaligned";
			}
			indices[name] = i;
		}
	}
	return null;
}

@property ref mark(MDB_val m)
	=> (cast(ubyte*)m.mv_data)[m.mv_size - 1];

MDB_dbi openDB(T)(MDB_txn* txn) @trusted {
	alias U = Unqual!T;
	enum flags = DBFlags.create | modelOf!U.flags |
		(getSerial!U == serial.invalid ? DBFlags.none : DBFlags.integerKey);
	MDB_dbi dbi = void;
	debug pragma(msg, "open " ~ modelOf!U.name);
	check(mdb_dbi_open(txn, modelOf!U.name, flags, &dbi));
	return dbi;
}

void intern(MDB_txn* txn, ref Val data) @trusted {
	import lmdb_orm.xxh3;

	XXH64_hash_t seed;
rehash:
	XXH64_hash_t[1] k = [xxh3_64Of(data, seed)];
	Val key = k[];
	Arr a = Arr(data);
	a.m.mv_size++;
	int rc = mdb_put(txn, openDB!Blob(txn), cast(MDB_val*)&key, &a.m,
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

void onUpdate(T)(MDB_txn* txn, CheckFlags flags, ref T obj) {
	import std.conv : text;

	if (flags & CheckFlags.empty) {
		foreach (i, ref x; obj.tupleof)
			checkEmpty!(T.tupleof[i])(x);
	}
	if (flags & CheckFlags.unique) {
		foreach (i, alias x; T.tupleof) {
			static if (hasUDA!(x, unique)) {
				Arr a;
				{
					mixin serialize!(obj, i, i + 1, false);
					setSize!true(a, obj, txn);
					const rc = mdb_put(txn, openDB!(UniqueIndex!(T, x))(txn), &bytes,
						&a.m, WriteFlags.noOverwrite | WriteFlags.reserve);
					if (rc == MDB_KEYEXIST) {
						alias K = Tuple!(TKey!T);
						const key = obj.tupleof[0 .. keyCount!T];
						const v = deserialize!K(a.m);
						if (key != v.tupleof)
							throw new DBException(text("Column " ~ fullyQualifiedName!x
									~ " must be unique ", " but found duplicate", key, " ", v));
					}
				}
				auto p = a.m.mv_data; // @suppress(dscanner.suspicious.unused_variable)
				mixin serialize!(obj, 0, keyCount!T, false);
			}
		}
	}
	// TODO: check foreign key constraints
	if (flags & CheckFlags.foreignTo)
		foreach (i, ref x; obj.tupleof) {
			foreach (a; __traits(getAttributes, T.tupleof[i])) {
				static if (is(a : FK!f, alias f))
					if (!exists!(__traits(parent, f), __traits(identifier, f))(txn, x))
						throw new DBException("Foreign key " ~ fullyQualifiedName!(
								T.tupleof[i]) ~ " not found");
			}
		}
}

template tryGet(alias key) {
	static if (isPK!x) {
		// TODO: check compound primary key
		const dbi = openDB!T(txn);
	} else {
		const dbi = openDB!(UniqueIndex!(T, x))(txn);
	}
	auto obj = Tuple!(typeof(key))(key);
	mixin serialize!(obj, 0, keyCount!T);
	Val val = void;
	const rc = mdb_get(txn, dbi, &bytes, cast(MDB_val*)&val);
}

unittest {
	import std.meta;
	import std.stdio;
	import std.string : cmp;

	remove("./db/test2/data.mdb");

	auto db = DB(256 << 10);
	db.open("./db/test2", EnvFlags.writeMap);
	auto txn = db.begin();
	txn.save(Company(1, "foo", "City A"));
	txn.save(Company(2, "bar", "City B"));
	string last;
	foreach (c; txn.cursor!(UniqueIndex!(Company, Company.name))) {
		assert(cmp(last, c.key) < 0);
		last = c.key;
		assert(txn.exists!(Company, "name")(c.key));
		assert(txn.exists!Company(c.val));
	}
	txn.save(User(0, "Alice", 1));
	txn.save(User(0, "Bob", 2));
	txn.commit();
	txn = db.begin(TxnFlags.readOnly);
	foreach (user; txn.cursor!User()) {
		writeln(user);
	}
	txn = db.begin();
	foreach (user; txn.mapper!User()) {
		user.companyID = 1;
	}
	txn.commit();
	txn = db.begin();
	writeln(txn.id);
	txn.del!User(1);
	txn.commit();
	txn = db.begin(TxnFlags.readOnly);
	foreach (user; txn.cursor!User()) {
		assert(user.companyID == 1);
	}
	txn = db.begin();
	foreach (user; txn.mapper!User()) {
		user.companyID = 2;
	}
	txn.abort();
	txn = db.begin(TxnFlags.readOnly);
	foreach (user; txn.cursor!User()) {
		writeln(user);
		assert(user.companyID == 1);
	}
}
