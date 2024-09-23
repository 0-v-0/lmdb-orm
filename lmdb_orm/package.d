module lmdb_orm;

public import lmdb_orm.orm;
public import lmdb_orm.traits;

struct LMDB {
private:
	Env env;
	MDB_txn* txn;
public:
	static int open(const char* path, int flags = 0, uint mode = defaultMode) {
	}

	int begin(const char* path, int flags = 0, int mode = 0) {
	}

	void close() {
		mdb_env_close(env);
	}
}
