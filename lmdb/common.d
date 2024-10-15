module lmdb.common;

/* Common header for all page types. The page type depends on #mp_flags.

  #P_BRANCH and #P_LEAF pages have unsorted '#MDB_node's at the end, with
  sorted #mp_ptrs[] entries referring to them. Exception: #P_LEAF2 pages
  omit mp_ptrs and pack sorted #MDB_DUPFIXED values after the page header.

  #P_OVERFLOW records occupy one or more contiguous pages where only the
  first has a page header. They hold the real data of #F_BIGDATA nodes.

  #P_SUBP sub-pages are small leaf "pages" with duplicate data.
  A node with flag #F_DUPDATA but not #F_SUBDATA contains a sub-page.
  (Duplicate data can also go in sub-databases, which use normal pages.)

  #P_META pages contain #MDB_meta, the start point of an LMDB snapshot.

  Each non-metapage up to #MDB_meta.%mm_last_pg is reachable exactly once
  in the snapshot: Either used by a database or listed in a freeDB record.
 */

struct MDB_page
{
	union
	{
		pgno_t p_pgno;	/**< page number */
		MDB_page* p_next;	/**< for in-memory list of freed pages */
	}
	 mp_p;
	uint16_t mp_pad;	/**< key size if this is a LEAF2 page */
	uint16_t mp_flags;	/**< @ref mdb_page */
	union
	{
		struct
		{
			indx_t pb_lower;	/**< lower bound of free space */
			indx_t pb_upper;	/**< upper bound of free space */
		}
		 pb;
		uint32_t pb_pages;	/**< number of overflow pages */
	}
	 mp_pb;
	indx_t[0] mp_ptrs;	/**< dynamic size */
}
/** Alternate page header, for 2-byte aligned access */
struct MDB_page2
{
	uint16_t[4] mp2_p;
	uint16_t mp2_pad;
	uint16_t mp2_flags;
	indx_t mp2_lower;
	indx_t mp2_upper;
	indx_t[0] mp2_ptrs;
}
