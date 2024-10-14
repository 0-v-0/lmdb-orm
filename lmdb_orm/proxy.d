module lmdb_orm.proxy;

import lmdb_orm.oo;
import lmdb_orm.traits;
import std.meta;
import std.traits;

private enum DIRTY = size_t(1) << (8 * size_t.sizeof - 1);

struct Proxy(T) if (isPOD!T) {
	package Cursor!T* _cur;
	private size_t _m;
	package this(ref Cursor!T c) {
		_cur = &c;
		_m = size_t.max & ~DIRTY;
	}

	~this() @trusted {
		import lmdb_orm.oo;

		if (_m & DIRTY) {
			auto flags = WriteFlags.current;
			const index = _m & ~DIRTY;
			if (index < T.tupleof.length) {
				enum end = offsets[$ - 1];
				size_t size = end;

				foreach (i, alias f; T.tupleof) {
					if (i >= index)
						static if (!isPK!f && isDynamicArray!(typeof(f))) {
							size += *cast(size_t*)(_cur.value.ptr + offsets[i]);
						}
				}
				// TODO: update
				if (size != _cur.value.length) {
					_cur.value.length = size;
				}
			}
			_cur.save(flags);
		}
	}

	@disable this(this);

@property:
	static foreach (i, alias f; T.tupleof) {
		static if (isPK!f) {
			alias a = _cur.key;
			alias filter = isPK;
		} else {
			alias a = _cur.value;
			alias filter = templateNot!isPK;
		}
		mixin("auto ", f.stringof, q{() @trusted {
			enum offsets = offsets!(T, filter);
			enum offset = offsets[i];
			assert(offset + f.sizeof <= a.length, "offset out of range");
			static if (isDynamicArray!(typeof(f))) {
				enum end = offsets[$ - 1];
				union Arr {
					typeof(f) v;
					struct {
						size_t length;
						size_t p;
					};
				}

				Arr res = *cast(Arr*)(a.ptr + offset);
				res.p += cast(size_t)a.ptr;
				return res.v;
			}
			return *cast(typeof(f)*)(a.ptr + offset);
		}});
		static if (!isPK!f)
			mixin("void ", f.stringof, q{(typeof(f) value) @trusted {
				enum offsets = offsets!(T, filter);
				enum offset = offsets[i];
				assert(offset + f.sizeof <= a.length, "offset out of range");
				if (_cur.checkFlags & CheckFlags.empty)
					checkEmpty!f(value);
				auto p = cast(typeof(f)*)(a.ptr + offset);
				static if (isDynamicArray!(typeof(f))) {
					if (i < (_m & ~DIRTY))
						_m = i | DIRTY;
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
	size_t[] res = [0];
	if (__ctfe)
		foreach (alias f; T.tupleof) {
			static if (filter!f) {
				res ~= res[$ - 1] + f.sizeof;
			}
		}
	return res;
}
