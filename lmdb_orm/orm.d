module lmdb_orm.orm;

import lmdb;
import std.exception;


struct Query {
private:
	MDB_txn* txn;
	MDB_dbi dbi;
public:
	int drop(bool del) => mdb_drop(txn, dbi, del);
	ulong insertID;
	ulong affected;
}
