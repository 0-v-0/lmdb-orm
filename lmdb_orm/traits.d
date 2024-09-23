module lmdb_orm.traits;

import std.meta;

/// Provide a custom name in the database for table
struct model {
	string name; /// The name of the table in the database
}

/// Mark a specific column as serial on the table
enum serial;

/// Mark a specific column as primary key on the table
enum PK;

/// Mark a specific column as unique on the table
enum unique;

/// foreign key
template foreign(alias field) {
}

package(lmdb_orm) enum isPOD(T) = __traits(isPOD, T);
