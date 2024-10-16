module lmdb_orm.proxy;

import lmdb_orm.orm;
import lmdb_orm.traits;
import std.meta;
import std.traits;

private enum DIRTY = size_t(1) << (8 * size_t.sizeof - 1);

/// Proxy for a database record
struct Proxy(T, bool readonly = false) if (isPOD!T) {
	package Cursor!(T, true)* _cur;

	private {
		static if (!readonly)
			size_t _m = size_t.max & ~DIRTY;
		enum _keyCount = keyCount!T;
		alias K = Tuple!(typeof(T.tupleof[0 .. _keyCount]));
		alias V = Tuple!(typeof(T.tupleof[_keyCount .. $]));
		template _Common() {
			static if (i < _keyCount) {
				const a = _cur.key;
				alias P = K;
				enum offset = P.tupleof[i].offsetof;
			} else {
				const a = _cur.val;
				alias P = V;
				enum offset = P.tupleof[i - _keyCount].offsetof;
			}
		}
	}

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
				_cur.save(flags);
			}
		}

	@disable this(this);

@property:
	static foreach (i, alias f; T.tupleof) {
		mixin("auto ", f.stringof, q{() @trusted {
			mixin _Common;
			assert(offset + f.sizeof <= a.length, "offset out of range");
			static if (isDynamicArray!(typeof(f))) {
				enum end = P.sizeof;

				Arr res = *cast(Arr*)(a.ptr + offset);
				if (end <= res.s[1] && res.s[1] <= a.length)
					res.s[1] += cast(size_t)a.ptr;
				else
					assert(0, "invalid pointer");
				return res.v;
			}
			return *cast(typeof(f)*)(a.ptr + offset);
		}});
		static if (!readonly && i >= _keyCount && isMutable!(typeof(f)))
			mixin("void ", f.stringof, q{(typeof(f) value) @trusted {
				mixin _Common;
				assert(offset + f.sizeof <= _cur.val.length, "offset out of range");
				if (_cur.checkFlags & CheckFlags.empty)
					checkEmpty!f(value);
				auto p = cast(typeof(f)*)(_cur.val.ptr + offset);
				static if (isDynamicArray!(typeof(f))) {
					if (i < (_m & ~DIRTY))
						_m = (i - _keyCount) | DIRTY;
				} else {
					_m |= DIRTY;
				}
				*p = value;
			}});
	}
}

unittest {
	import std.stdio;

}

private:
auto offsets(T, alias filter)() {
	size_t[Filter!(filter, T.tupleof).length] res;
	if (__ctfe)
		foreach (i, alias f; T.tupleof) {
			static if (filter!f) {
				res[i] = f.offsetof;
			}
		}
	return res;
}
