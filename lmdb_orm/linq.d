module lmdb_orm.linq;
private:

void pop(R)(ref R a, ref R b) {
	const x = a.front;
	const y = b.front;
	if (x < y) {
		a.popFront();
	} else if (y < x) {
		b.popFront();
	} else {
		a.popFront();
		b.popFront();
	}
}

template Ctor(R) {
	R a, b;

	this(R a, R b) {
		this.a = a;
		this.b = b;
	}
}

extern (C++):

class Intersect(R) {
	mixin Ctor!R;

	@property auto ref front() => a.front;

	@property bool empty() {
		while (!a.empty && !b.empty) {
			const x = a.front;
			const y = b.front;
			if (x < y) {
				a.popFront();
			} else if (y < x) {
				b.popFront();
			} else {
				return false;
			}
		}
		return true;
	}

	auto popFront() {
		while (!a.empty && !b.empty) {
			const x = a.front;
			const y = b.front;
			if (x < y) {
				a.popFront();
			} else if (y < x) {
				b.popFront();
			} else {
				a.popFront();
				b.popFront();
				break;
			}
		}
	}
}

auto intersect(R)(R a, R b)
	=> new Intersect!R(a, b);

unittest {
	import std.algorithm;
	import std.range;

	auto e = intersect(iota(0, 0), iota(2, 5));
	assert(e.empty);
	auto x = intersect(iota(0, 10), iota(5, 15));
	assert(x.equal(iota(5, 10)));
	auto y = intersect(iota(0, 10, 2), iota(1, 10, 2));
	assert(y.empty);
}

class Union(R) {
	mixin Ctor!R;

	@property auto ref front() {
		if (a.empty) {
			return b.front;
		}
		const x = a.front;
		if (b.empty) {
			return x;
		}
		const y = b.front;
		return x < y ? x : y;
	}

	@property bool empty() => a.empty && b.empty;

	auto popFront() {
		if (a.empty) {
			b.popFront();
		} else if (b.empty) {
			a.popFront();
		} else {
			pop(a, b);
		}
	}
}

auto union_(R)(R a, R b)
	=> new Union!R(a, b);

unittest {
	import std.algorithm;
	import std.range;

	auto e = union_(iota(0, 0), iota(2, 5));
	assert(e.equal(iota(2, 5)));
	auto x = union_(iota(0, 10), iota(5, 15));
	assert(x.equal(iota(0, 15)));
	auto y = union_(iota(0, 10, 2), iota(1, 10, 2));
	assert(y.equal(iota(0, 10, 1)));
}

class Diff(R) {
	mixin Ctor!R;

	@property auto ref front() => a.front;

	@property bool empty() {
		while (!a.empty && !b.empty) {
			const x = a.front;
			const y = b.front;
			if (x < y) {
				return false;
			}
			if (x == y)
				a.popFront();
			b.popFront();
		}
		return a.empty;
	}

	auto popFront() {
		if (!a.empty) {
			if (b.empty)
				a.popFront();
			else
				pop(a, b);
		}
	}
}

auto diff(R)(R a, R b)
	=> new Diff!R(a, b);

unittest {
	import std.algorithm;
	import std.range;

	auto e = diff(iota(0, 0), iota(2, 5));
	assert(e.equal(iota(0, 0)));
	auto x = diff(iota(0, 10), iota(5, 15));
	assert(x.equal(iota(0, 5)));
	auto y = diff(iota(0, 10, 2), iota(1, 10, 2));
	assert(y.equal(iota(0, 10, 2)));
}

class SymDiff(R) {
	mixin Ctor!R;

	@property auto ref front() {
		if (a.empty) {
			return b.front;
		}
		const x = a.front;
		if (b.empty) {
			return x;
		}
		const y = b.front;
		return x < y ? x : y;
	}

	@property bool empty() {
		while (!a.empty && !b.empty) {
			const x = a.front;
			const y = b.front;
			if (x != y)
				return false;
			a.popFront();
			b.popFront();
		}
		return a.empty && b.empty;
	}

	auto popFront() {
		if (a.empty) {
			b.popFront();
		} else if (b.empty) {
			a.popFront();
		} else {
			pop(a, b);
		}
	}
}

auto symDiff(R)(R a, R b)
	=> new SymDiff!R(a, b);

unittest {
	import std.algorithm;
	import std.range;

	auto e = symDiff(iota(0, 0), iota(2, 5));
	assert(e.equal(iota(2, 5)));
	auto x = symDiff(iota(0, 10), iota(5, 15));
	assert(x.equal([0, 1, 2, 3, 4, 10, 11, 12, 13, 14]));
	auto y = symDiff(iota(0, 10, 2), iota(1, 10, 2));
	assert(y.equal(iota(0, 10, 1)));
}
