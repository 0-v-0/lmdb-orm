module lmdb_orm.oo;

import lmdb_orm.lmdb;
import std.conv : octal;
import std.string;

version (unittest) {
	import std.stdio;
}

private struct NonNull(T) {
	alias value this;
	@property T value() => val;

private:
	T val;
	this(T x)
	in (x !is null) {
		val = x;
	}
}

private alias CO = MDB_cursor_op;

/** -rw-r--r-- */
enum defaultMode = octal!644;

alias Env = NonNull!(MDB_env*);
alias Txn = NonNull!(MDB_txn*);
alias Val = const(void)[];
private enum opOffset = MDB_LAST_ERRCODE + 1;

struct Cursor {
	import std.typecons : tuple;

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

	void popFront() @trusted {
		rc = mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, CursorOp.next);
	}

	void popBack() @trusted {
		rc = mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, CursorOp.prev);
	}

	/// Retrieve by cursor.
	int get(ref Val key, ref Val val, CursorOp op = CursorOp.next)
		=> mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, op);

	/// Store by cursor.
	int set(const ref Val key, const ref Val val, WriteFlags flags = WriteFlags.none)
		=> mdb_cursor_put(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, flags);

	/// Delete by cursor.
	int del(DeleteFlags flags = DeleteFlags.none)
		=> mdb_cursor_del(cursor, flags);

@property:
	/// Return count of duplicates for current key.
	size_t count() @trusted {
		size_t count = void;
		check(mdb_cursor_count(cursor, &count));
		return count;
	}

	/// Return the cursor's database handle.
	LMDB dbi() @trusted
		=> LMDB(Txn(cursor.txn), mdb_cursor_dbi(cursor));

	bool empty() @trusted {
		if (opOffset <= rc && rc <= opOffset + CursorOp.max)
			rc = mdb_cursor_get(cursor, cast(MDB_val*)&key, cast(MDB_val*)&val, cast(MDB_cursor_op)(
					rc - opOffset));
		if (rc != MDB_NOTFOUND)
			check(rc);
		return rc == MDB_NOTFOUND;
	}

	auto ref front() => tuple(key, val);
	alias back = front;
}

/** Return the LMDB library version information.
Params:
major = if non-NULL, the library major version number is copied here
minor = if non-NULL, the library minor version number is copied here
patch = if non-NULL, the library patch version number is copied here

Returns: "version string" The library version as a string
 */
string getVersion(int* major = null, int* minor = null, int* patch = null) @trusted
	=> cast(string)fromStringz(mdb_version(major, minor, patch));

unittest {
	writeln(getVersion());
}

/** Return a string describing a given error code.

This function is a superset of the ANSI C X3.159-1989 (ANSI C) strerror(3)
function. If the error code is greater than or equal to 0, then the string
returned by the system function strerror(3) is returned. If the error code
is less than 0, an error string corresponding to the LMDB library error is
returned. See @ref errors for a list of LMDB-specific error codes.
Params:
err = The error code
Returns: "error message" The description of the error
 */
string errorString(int err) @trusted
	=> cast(string)fromStringz(mdb_strerror(err));

unittest {
	writeln(errorString(MDB_NOTFOUND));
}

/** Base class for LMDB exceptions */
class MbdError : Exception {
	import std.exception;

	mixin basicExceptionCtors;
}

/** Check for LMDB errors and throw an exception if one is found.
This function checks the return code from an LMDB function call and throws an
exception if an error occurred.
Params:
origin = The origin of the LMDB function call (for error messages)
rc = The return code from an LMDB function call
 */
void check(string origin = __FUNCTION__)(int rc) {
	import std.conv : text;

	if (rc) {
		auto msg = origin ~ ": " ~ errorString(rc) ~ text(" (", rc, ")");
		throw new MbdError(msg);
	}
}

/** Environment Flags */
enum EnvFlags : uint {
	none,
	/** mmap at a fixed address (experimental) */
	fixedMap = MDB_FIXEDMAP,
	/** no environment directory */
	noSubdir = MDB_NOSUBDIR,
	/** don't fsync after commit */
	noSync = MDB_NOSYNC,
	/** read only */
	readOnly = MDB_RDONLY,
	/** don't fsync metapage after commit */
	noMetaSync = MDB_NOMETASYNC,
	/** use writable mmap */
	writeMap = MDB_WRITEMAP,
	/** use asynchronous msync when #MDB_WRITEMAP is used */
	mapAsync = MDB_MAPASYNC,
	/** tie reader locktable slots to #MDB_txn objects instead of to threads */
	noTLS = MDB_NOTLS,
	/** don't do any locking, caller must manage their own locks */
	noLock = MDB_NOLOCK,
	/** don't do readahead (no effect on Windows) */
	noReadAhead = MDB_NORDAHEAD,
	/** don't initialize malloc'd memory before writing to datafile */
	noMemInit = MDB_NOMEMINIT,
}

/** Transaction Flags */
enum TxnFlags : EnvFlags {
	none,
	readOnly = EnvFlags.readOnly,
}

/** Database Flags */
enum DBFlags : uint {
	none,
	/** use reverse string keys */
	reverseKey = MDB_REVERSEKEY,
	/** use sorted duplicates */
	dupSort = MDB_DUPSORT,
	/** numeric keys in native byte order: either unsigned int or size_t.
	 *  The keys must all be of the same size. */
	integerKey = MDB_INTEGERKEY,
	/** with #MDB_DUPSORT, sorted dup items have fixed size */
	dupFixed = MDB_DUPFIXED,
	/** with #MDB_DUPSORT, dups are #MDB_INTEGERKEY-style integers */
	integerDup = MDB_INTEGERDUP,
	/** with #MDB_DUPSORT, use reverse string dups */
	reverseDup = MDB_REVERSEDUP,
	/** create DB if not already existing */
	create = MDB_CREATE,
}

/** Write Flags */
enum WriteFlags : uint {
	none,
	/** Don't write if the key already exists */
	noOverwrite = MDB_NOOVERWRITE,
	/** Don't write if the key and data pair already exist */
	noDupData = MDB_NODUPDATA,
	/** Overwrite the current key/data pair */
	current = MDB_CURRENT,
	/** Just reserve space for data, don't copy it. Return a pointer to the reserved space */
	reserve = MDB_RESERVE,
	/** Data is being appended, don't split full pages */
	append = MDB_APPEND,
	/** Duplicate data is being appended, don't split full pages */
	appendDup = MDB_APPENDDUP,
	/** Store multiple data items in one call. Only for #MDB_DUPFIXED */
	multiple = MDB_MULTIPLE,
}

/** Delete Flags */
enum DeleteFlags : WriteFlags {
	none,
	/** delete all of the data items for the current key.
    	This flag may only be specified if the database was opened with #MDB_DUPSORT. */
	noDupData = WriteFlags.noDupData,
}

/** Cursor Operations */
enum CursorOp : CO {
	first = CO.MDB_FIRST, /** Position at first key/data item */
	firstDup = CO.MDB_FIRST_DUP, /** Position at first data item of current key.
                                    Only for #MDB_DUPSORT */
	getBoth = CO.MDB_GET_BOTH, /** Position at key/data pair. Only for #MDB_DUPSORT */
	getBothRange = CO.MDB_GET_BOTH_RANGE, /** position at key, nearest data. Only for #MDB_DUPSORT */
	getCurrent = CO.MDB_GET_CURRENT, /** Return key/data at current cursor position */
	getMultiple = CO.MDB_GET_MULTIPLE, /** Return up to a page of duplicate data items
							from current cursor position. Move cursor to prepare
							for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
	last = CO.MDB_LAST, /** Position at last key/data item */
	lastDup = CO.MDB_LAST_DUP, /** Position at last data item of current key.
                                    Only for #MDB_DUPSORT */
	next = CO.MDB_NEXT, /** Position at next data item */
	nextDup = CO.MDB_NEXT_DUP, /** Position at next data item of current key.
						Only for #MDB_DUPSORT */
	nextMultiple = CO.MDB_NEXT_MULTIPLE, /** Return up to a page of duplicate data items
						from next cursor position. Move cursor to prepare
						for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
	nextNoDup = CO.MDB_NEXT_NODUP, /** Position at first data item of next key */
	prev = CO.MDB_PREV, /** Position at previous data item */
	prevDup = CO.MDB_PREV_DUP, /** Position at previous data item of current key.
                                    Only for #MDB_DUPSORT */
	prevNoDup = CO.MDB_PREV_NODUP, /** Position at last data item of previous key */
	set = CO.MDB_SET, /** Position at specified key */
	setKey = CO.MDB_SET_KEY, /** Position at specified key, return key + data */
	setRange = CO.MDB_SET_RANGE, /** Position at first key greater than or equal to specified key. */
	prevMultiple = CO.MDB_PREV_MULTIPLE /** Position at previous page and return up to
							a page of duplicate data items. Only for #MDB_DUPFIXED */
}

struct EnvInfo {
	void* mapaddr; /** Address of map, if fixed */
	size_t mapsize; /** Size of data memory map */
	size_t lastPage; /** ID of last used page */
	size_t lastTxnid; /** ID of last committed transaction */
	uint maxreaders; /** max reader slots in the environment */
	uint numreaders; /** max reader slots used in the environment */
}

struct Stat {
	uint psize; /** Size of a database page in bytes.
 					This is currently the same for all databases. */
	uint depth; /** Depth (height) of the B-tree */
	size_t branchPages; /** Number of internal (non-leaf) pages */
	size_t leafPages; /** Number of leaf pages */
	size_t overflowPages; /** Number of overflow pages */
	size_t entries; /** Number of data items */
}

/** Create a new environment handle.
Returns: a new environment handle
 */
Env create() @trusted {
	MDB_env* env = void;
	check(mdb_env_create(&env));
	return Env(env);
}

/** Close an environment handle.
Params:
env = the environment handle
 */
alias close = mdb_env_close;

/** Open an environment handle.
Params:
env = the environment handle
path = the directory in which the database files reside
flags = optional flags for this environment. This parameter is
a bitwise OR of the values described above.
mode = the UNIX permissions to set on created files. This parameter
is ignored on Windows.
Returns: 0 on success, non-zero on failure.
 */
int open(Env env, scope const char* path, EnvFlags flags = EnvFlags.none,
	ushort mode = defaultMode) @trusted
	=> mdb_env_open(env, path, flags, mode);

/** Get environment info.
Params:
env = the environment handle
Returns: the environment info
 */
@property EnvInfo envinfo(Env env) @trusted {
	EnvInfo info = void;
	check(mdb_env_info(env, cast(MDB_envinfo*)&info));
	return info;
}

/** Get environment statistics.
Params:
env = the environment handle
Returns: the environment statistics
 */
@property Stat stat(Env env) @trusted {
	Stat info = void;
	check(mdb_env_stat(env, cast(MDB_stat*)&info));
	return info;
}

/** Get environment flags.
Params:
env = the environment handle
Returns: the environment flags
 */
@property uint flags(Env env) @trusted {
	uint flags = void;
	check(mdb_env_get_flags(env, &flags));
	return flags;
}

/** Get the path of the environment.
Params:
env = the environment handle
Returns: the path
 */
@property string path(Env env) @trusted {
	char* path = void;
	check(mdb_env_get_path(env, &path));
	return cast(string)fromStringz(path);
}

/** Get the file descriptor for the environment.
Params:
env = the environment handle
Returns: the file descriptor
 */
@property auto fd(Env env) @trusted {
	FileHandle fd = void;
	check(mdb_env_get_fd(env, &fd));
	return fd;
}

/** Copy an LMDB environment to the specified path.
Params:
env = the environment handle
path = the path to which the environment should be copied
flags = optional flags for this operation. This parameter is unused and
must be set to 0.
Returns: 0 on success, non-zero on failure.
 */
alias copy = mdb_env_copy;

/// ditto
int copy(Env env, scope const char* path, bool compact)
	=> mdb_env_copy2(env, path, compact);

alias FileHandle = mdb_filehandle_t;

/// ditto
int copy(Env env, FileHandle fd)
	=> mdb_env_copyfd(env, fd);

/// ditto
int copy(Env env, FileHandle fd, bool compact)
	=> mdb_env_copyfd2(env, fd, compact);

/** Flush the data buffers to disk.
Params:
env = the environment handle
force = if non-zero, force a synchronous flush
Returns: 0 on success, non-zero on failure.
 */
int sync(Env env, bool force = false)
	=> mdb_env_sync(env, force);

/// Get/set the application information
alias userctx = mdb_env_get_userctx;

/// ditto
@property Env userctx(Env env, void* ctx) {
	check(mdb_env_set_userctx(env, ctx));
	return env;
}

/** Get the maximum number of threads for the environment.
Params:
env = the environment handle
Returns: the maximum number of threads allowed
 */
@property uint maxreaders(Env env) @trusted {
	uint readers = void;
	check(mdb_env_get_maxreaders(env, &readers));
	return readers;
}

/** Set the maximum number of threads for the environment.
Params:
env = the environment handle
readers = the maximum number of threads to allow
 */
@property Env maxreaders(Env env, uint readers) {
	check(mdb_env_set_maxreaders(env, readers));
	return env;
}

/** Set the maximum number of databases for the environment.
Params:
env = the environment handle
maxdbs = the maximum number of databases to allow
 */
@property Env maxdbs(Env env, uint maxdbs) {
	check(mdb_env_set_maxdbs(env, maxdbs));
	return env;
}

/** Set the size of the memory map to use for this environment.
Params:
env = the environment handle
size = the size of the memory map
 */
@property Env mapsize(Env env, size_t size) {
	check(mdb_env_set_mapsize(env, size));
	return env;
}

/** Check for stale entries in the reader lock table.
Params:
env = the environment handle
Returns: dead Number of stale slots that were cleared
 */
int readerCheck(Env env) @trusted {
	int dead = void;
	check(mdb_reader_check(env, &dead));
	return dead;
}

/** Begin a transaction.
Params:
env = the environment handle
flags = optional transaction flags.
parent = handle of a transaction that may be a parent of the new transaction.
Returns: a transaction handle
 */
Txn begin(Env env, TxnFlags flags = TxnFlags.none, MDB_txn* parent = null) @trusted {
	MDB_txn* txn = void;
	check(mdb_txn_begin(env, parent, flags, &txn));
	return Txn(txn);
}

/** Begin a transaction.
Params:
parent = handle of a transaction that may be a parent of the new transaction.
flags = optional transaction flags.
Returns: a transaction handle
 */
Txn begin(Txn parent, TxnFlags flags = TxnFlags.none) @trusted {
	MDB_txn* txn = void;
	check(mdb_txn_begin(parent.env, parent, flags, &txn));
	return Txn(txn);
}

alias env = mdb_txn_env;

alias id = mdb_txn_id;

alias commit = mdb_txn_commit;
alias abort = mdb_txn_abort;
alias reset = mdb_txn_reset;
alias renew = mdb_txn_renew;

/** Open a database in the environment.
Params:
txn = a transaction handle returned by #mdb_txn_begin
name = the name of the database to open.
flags = optional flags for this database.
Returns: a database handle
 */
LMDB open(Txn txn, scope const char* name, DBFlags flags = DBFlags.none) @trusted {
	MDB_dbi dbi = void;
	check(mdb_dbi_open(txn, name, flags, &dbi));
	return LMDB(txn, dbi);
}

/// A database handle
struct LMDB {
	package Txn txn;
	/// The database handle
	const MDB_dbi dbi;

	/// Get the database flags.
	@property DBFlags flags() @trusted {
		uint flags = void;
		check(mdb_dbi_flags(txn, dbi, &flags));
		return cast(DBFlags)flags;
	}

	/// Get the database statistics.
	@property Stat stat() @trusted {
		Stat info = void;
		check(mdb_stat(txn, dbi, cast(MDB_stat*)&info));
		return info;
	}

	/** Get items from the database.
	Params:
		key = the key to search for
		Returns: the data item for that key
	 */
	Val get(const ref Val key) @trusted {
		MDB_val val = void;
		check(mdb_get(txn, dbi, cast(MDB_val*)&key, &val));
		return *cast(Val*)&val;
	}

	/** Get items from the database.
	Params:
		key = the key to search for
		defValue = the default value to return if the key is not found
		Returns: the data item for that key
	 */
	Val get(const ref Val key, in Val defValue) @trusted {
		MDB_val val = void;
		int rc = mdb_get(txn, dbi, cast(MDB_val*)&key, &val);
		if (rc == MDB_NOTFOUND)
			return defValue;
		check(rc);
		return *cast(Val*)&val;
	}

	/// Get a cursor handle.
	Cursor cursor(CursorOp op = CursorOp.next) @trusted {
		MDB_cursor* cursor = void;
		check(mdb_cursor_open(txn, dbi, &cursor));
		return Cursor(cursor, op);
	}

nothrow:
	/** Set items in the database.
	Params:
		key = the key to store
		val = the data item to store
		flags = optional flags for this operation.
	Returns: 0 on success, non-zero on failure.
	 */
	int set(const ref Val key, const ref Val val, WriteFlags flags = WriteFlags.none)
		=> mdb_put(txn, dbi, cast(MDB_val*)&key, cast(MDB_val*)&val, flags);

	/// ditto
	int set(in Val key, in Val val, WriteFlags flags = WriteFlags.none)
		=> mdb_put(txn, dbi, cast(MDB_val*)&key, cast(MDB_val*)&val, flags);

	/** Delete items from the database.
	Params:
		key = the key to delete
		val = the data item to delete, if any
	Returns: 0 on success, non-zero on failure.
	 */
	int del(const ref Val key)
		=> mdb_del(txn, dbi, cast(MDB_val*)&key, null);

	/// ditto
	int del(in Val key)
		=> mdb_del(txn, dbi, cast(MDB_val*)&key, null);

	/// ditto
	int del(const ref Val key, const ref Val val)
		=> mdb_del(txn, dbi, cast(MDB_val*)&key, cast(MDB_val*)&val);

	/// ditto
	int del(in Val key, in Val val)
		=> mdb_del(txn, dbi, cast(MDB_val*)&key, cast(MDB_val*)&val);

	/// Close the database handle.
	void close() => mdb_dbi_close(txn.env, dbi);

	/** Empty or delete+close a database.
	Params:
		del = if true, delete the database in addition to closing it
	Returns: 0 on success, non-zero on failure.
	 */
	int drop(bool del = false) => mdb_drop(txn, dbi, del);
}

alias txn = mdb_cursor_txn;

/// Renew a cursor handle.
alias renew = mdb_cursor_renew;

/// Close a cursor handle.
alias close = mdb_cursor_close;

/// Get the maximum size of keys
alias maxkeysize = mdb_env_get_maxkeysize;

unittest {
	Env env = create();
	scope (exit)
		close(env);
	env.mapsize = 256 << 10;
	env.maxdbs = 2;
	check(env.open("./test", EnvFlags.fixedMap | EnvFlags.noSubdir | EnvFlags.writeMap));
	writeln("maxreaders: ", env.maxreaders);
	writeln("maxkeysize: ", env.maxkeysize);
	writeln("flags: ", env.flags);
	writeln("envinfo: ", env.envinfo);
	writeln("stat: ", env.stat);
	check(env.sync());
	Txn txn = env.begin();
	writeln("id: ", txn.id);
	LMDB db = txn.open("test", DBFlags.create);
	writeln("dbi: ", db.dbi);
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
