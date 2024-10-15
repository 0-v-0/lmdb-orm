module lmdb.env;

import core.stdc.stdint;

alias HANDLE = void*;

/** The database environment. */
struct MDB_env {
	HANDLE me_fd; /**< The main data file */
	HANDLE me_lfd; /**< The lock file */
	HANDLE me_mfd; /**< For writing and syncing the meta pages */
	uint32_t me_flags; /**< @ref mdb_env */
	uint me_psize; /**< DB page size, inited from me_os_psize */
	uint me_os_psize; /**< OS page size, from #GET_PAGESIZE */
	uint me_maxreaders; /**< size of the reader table */
	/** Max #MDB_txninfo.%mti_numreaders of interest to #mdb_env_close() */
	int me_close_readers;
	MDB_dbi me_numdbs; /**< number of DBs opened */
	MDB_dbi me_maxdbs; /**< size of the DB table */
	int me_pid; /**< process ID of this env */
	char* me_path; /**< path to the DB files */
	char* me_map; /**< the memory map of the data file */
	MDB_txninfo* me_txns; /**< the memory map of the lock file or NULL */
	MDB_meta*[2] me_metas; /**< pointers to the two meta pages */
	void* me_pbuf; /**< scratch area for DUPSORT put() */
	MDB_txn* me_txn; /**< current write transaction */
	MDB_txn* me_txn0; /**< prealloc'd write transaction */
	size_t me_mapsize; /**< size of the data memory map */
	off_t me_size; /**< current file size */
	pgno_t me_maxpg; /**< me_mapsize / me_psize */
	MDB_dbx* me_dbxs; /**< array of static DB info */
	uint16_t* me_dbflags; /**< array of flags from MDB_db.md_flags */
	uint* me_dbiseqs; /**< array of dbi sequence numbers */
	DWORD me_txkey; /**< thread-key for readers */
	txnid_t me_pgoldest; /**< ID of oldest reader last time we looked */
	MDB_pgstate me_pgstate; /**< state of old pages from freeDB */
	MDB_page* me_dpages; /**< list of malloc'd blocks for re-use */
	/** IDL of pages that became unused in a write txn */
	MDB_IDL me_free_pgs;
	/** ID2L of pages written during a write txn. Length MDB_IDL_UM_SIZE. */
	MDB_ID2L me_dirty_list;
	/** Max number of freelist items that can fit in a single overflow page */
	int me_maxfree_1pg;
	/** Max size of a node on a page */
	uint me_nodemax;
	int me_live_reader; /**< have liveness lock in reader table */
	int me_pidquery; /**< Used in OpenProcess */
	mdb_mutex_t me_rmutex;
	mdb_mutex_t me_wmutex;
	void* me_userctx; /**< User-settable context */
}

/// Information about a single database in the environment.
struct MDB_db {
	uint32_t md_pad; /**< also ksize for LEAF2 pages */
	uint16_t md_flags; /**< @ref mdb_dbi_open */
	uint16_t md_depth; /**< depth of this tree */
	pgno_t md_branch_pages; /**< number of internal pages */
	pgno_t md_leaf_pages; /**< number of leaf pages */
	pgno_t md_overflow_pages; /**< number of overflow pages */
	size_t md_entries; /**< number of data items */
	pgno_t md_root; /**< the root page of this tree */
}

/** Auxiliary DB info.
 *	The information here is mostly static/read-only. There is
 *	only a single copy of this record in the environment.
 */
struct MDB_dbx {
	MDB_val md_name; /**< name of the database */
	MDB_cmp_func md_cmp; /**< function for comparing keys */
	MDB_cmp_func md_dcmp; /**< function for comparing data items */
}
