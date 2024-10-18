module app;

import std.meta;
import std.stdio;
import lmdb_orm;

@model
struct User {
	@serial long id;
	@unique @nonEmpty string name;
	@FK!(Company.id) long companyID;
	long createdAt;
	long updatedAt;

	static void onSave(T)(ref T user, scope Save next) {
		if (!user.createdAt)
			user.createdAt = now();
		user.updatedAt = now();
		next();
	}
}

@model
struct Company {
	@serial long id;
	@unique @nonEmpty string name;
	string address;

	static bool onDelete(long id, scope Del next) {
		writeln("Company ", id, " is deleted");
		return next();
	}
}

enum RelationType {
	friend,
	colleague,
	enemy,
}

@model
struct Relation {
	@PK @FK!(User.id) {
		long userA;
		long userB;
	}
	RelationType type;

	static void onSave(T)(ref T r, scope Next next) {
		if (r.type < RelationType.min || r.type > RelationType.max)
			throw new Exception("Invalid relation type");

		next();
	}
}

@property auto now() {
	import std.datetime;

	try {
		return Clock.currStdTime;
	} catch (Exception) {
		return long.max;
	}
}

alias DB = FSDB!(app);

void main() {
    alias modules = AliasSeq!(app);
    auto db = DB(256 << 10);
    db.open("../db/app", EnvFlags.writeMap);
    auto txn = db.begin();
    txn.save(User(0, "Alice", 0));
    txn.save(User(0, "Bob", 1));
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
}
