module lmdb_orm.oo;

import lmdb;
import std.conv;
import std.string;

/**< -rw-r--r-- */
enum defaultMode = octal!644;

/** Return the LMDB library version information.
Params:
major = if non-NULL, the library major version number is copied here
minor = if non-NULL, the library minor version number is copied here
patch = if non-NULL, the library patch version number is copied here

Returns: "version string" The library version as a string
 */
string getVersion(int* major = null, int* minor = null, int* patch = null) {
	return fromStringz(lmdb_version(major, minor, patch));
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
string errorString(int err) {
	return fromStringz(lmdb_strerror(err));
}

/** Base class for LMDB exceptions */
class MbdError : Exception {
	mixin basicExceptionCtors;
}

package void check(string origin = __FUNCTION__)(int rc) {
	if (rc) {
		auto msg = origin ~ ": " ~ errorString(rc);
		throw new MbdError(msg);
	}
}

/** Environment Flags */
enum EnvFlags : uint {
	/** mmap at a fixed address (experimental) */
	fixedMap = MDB_FIXEDMAP,
	/** no environment directory */
	noSubDir = MDB_NOSUBDIR,
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
	noMemInit = MDB_NOMEMINIT
}

/** Database Flags */
enum DBFlags : uint {
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
	create = MDB_CREATE
}

/** Write Flags */
enum WriteFlags : uint {
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
	multiple = MDB_MULTIPLE
}

/** Cursor Operations */
enum CursorOp {
	first = 0, /**< Position at first key/data item */
	firstDup = 1, /**< Position at first data item of current key.
                                    Only for #MDB_DUPSORT */
	getBoth = 2, /**< Position at key/data pair. Only for #MDB_DUPSORT */
	getBothRange = 3, /**< position at key, nearest data. Only for #MDB_DUPSORT */
	getCurrent = 4, /**< Return key/data at current cursor position */
	getMultiple = 5, /**< Return up to a page of duplicate data items
							from current cursor position. Move cursor to prepare
							for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
	last = 6, /**< Position at last key/data item */
	lastDup = 7, /**< Position at last data item of current key.
                                    Only for #MDB_DUPSORT */
	next = 8, /**< Position at next data item */
	nextDup = 9, /**< Position at next data item of current key.
						Only for #MDB_DUPSORT */
	nextMultiple = 10, /**< Return up to a page of duplicate data items
						from next cursor position. Move cursor to prepare
						for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
	nextNoDup = 11, /**< Position at first data item of next key */
	prev = 12, /**< Position at previous data item */
	prevDup = 13, /**< Position at previous data item of current key.
                                    Only for #MDB_DUPSORT */
	prevNoDup = 14, /**< Position at last data item of previous key */
	set = 15, /**< Position at specified key */
	setKey = 16, /**< Position at specified key, return key + data */
	setRange = 17, /**< Position at first key greater than or equal to specified key. */
	prevMultiple = 18 /**< Position at previous page and return up to
							a page of duplicate data items. Only for #MDB_DUPFIXED */
}

uint flags(Env env) @trusted
in (env) {
	uint flags = void;
	check(mdb_env_get_flags(env, &flags));
	return flags;
}

alias copy = mdb_env_copy;

int sync(Env env, bool force = false)
in (env)
	=> mdb_env_sync(env, force);

alias userctx = mdb_env_get_userctx;

Env userctx(Env env, void* ctx)
in (env) {
	check(mdb_env_set_userctx(env, ctx));
	return env;
}

alias maxkeysize = mdb_env_get_maxkeysize;
