module lmdb_orm.orm;

struct Query {
private:
	//MDB_txn* txn;
	//MDB_dbi dbi;
public:
	ulong insertID;
	ulong affected;
}
