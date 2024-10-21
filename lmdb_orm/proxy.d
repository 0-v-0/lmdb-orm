module lmdb_orm.proxy;

import lmdb_orm.orm;
import lmdb_orm.traits;
import std.meta;
import std.traits;

private template Def(bool readonly) {
	import lmdb_orm.oo;

	private {
		static if (!readonly)
			size_t _m = size_t.max & ~DIRTY;
		enum _keyCount = keyCount!T;
		alias K = Tuple!(typeof(T.tupleof[0 .. _keyCount]));
		alias V = Tuple!(typeof(T.tupleof[_keyCount .. $]));
	}

	@disable this(this);

	static if (!readonly)
		 ~this() @trusted {
			import lmdb_orm.oo;

			if (_m & DIRTY) {
				auto flags = WriteFlags.current;
				const index = _m & ~DIRTY;
				if (index < T.tupleof.length) {
					size_t size = V.sizeof;

					foreach (i, alias f; V.tupleof) {
						static if (i >= _keyCount) {
							if (i >= index)
								static if (isDynamicArray!(typeof(f)))
									size += *cast(size_t*)(
										_cur.val.ptr + f.offsetof) * typeof(f[0]).sizeof;
						}
					}
					// TODO: update
					if (size != _cur.val.length) {
						assert(0, "unimplemented");
					}
				}
				scope Save next = () @trusted { _cur.save(flags); };
				mixin CallNext!(T, "onSave", this, next);
			}
		}

	template opDispatch(string member) if (__traits(hasMember, T, member)) {
		alias f = __traits(getMember, T, member);
		alias F = typeof(f);
		enum i = staticIndexOf!(f, T.tupleof);
		alias P = AliasSeq!(K, V)[i >= _keyCount];
		enum offset = P.tupleof[i < _keyCount ? i: i - _keyCount].offsetof;
	@property:
		auto opDispatch() @trusted {
			mixin Common;
			assert(offset + f.sizeof <= a.length, "offset out of range");
			static if (isDynamicArray!(F)) {
				enum end = P.sizeof;

				Arr res = *cast(Arr*)(a.ptr + offset);
				if (end <= res.s[1] && res.s[1] <= a.length)
					res.s[1] += cast(size_t)a.ptr;
				else
					assert(0, "invalid pointer");
				return res.v;
			}
			return *cast(F*)(a.ptr + offset);
		}

		static if (!readonly && !isPK!f && isMutable!(F))
			auto opDispatch(F value) @trusted {
				mixin Common;
				assert(offset + f.sizeof <= _cur.val.length, "offset out of range");
				if (_cur.checkFlags & CheckFlags.empty)
					checkEmpty!f(value);
				auto p = cast(F*)(_cur.val.ptr + offset);
				static if (isDynamicArray!(F)) {
					if (i < (_m & ~DIRTY))
						_m = (i - _keyCount) | DIRTY;
				} else {
					_m |= DIRTY;
				}
				*p = value;
			}
	}
}

/// readonly proxy for a database record
struct Proxy(T) if (isPOD!T) {
	mixin KV _cur;
	mixin Def!true;
}

/// Proxy for a database record
struct Proxy(T, bool readonly) if (isPOD!T) {
	private Cursor!(T, true)* _cur;

	mixin Def!readonly;
}

/// Remove a record from the database
void remove(T)(Proxy!(T, false) p) {
	p._cur.del();
}

private:

enum DIRTY = size_t(1) << (8 * size_t.sizeof - 1);
template Common() {
	static if (i < _keyCount) {
		const a = _cur.key;
	} else {
		const a = _cur.val;
	}
}

template KV() {
	import lmdb_orm.oo;

private:
	Val key;
	Val val;
}
