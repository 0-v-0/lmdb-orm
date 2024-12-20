module lmdb_orm.linq;

import std.traits;
import tame.unsafe.scoped;

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

public:

extern (C++):

interface Iterator(T) {
	@property T front();
	@property bool empty();
	void popFront();
}

class Intersect(R, E = ForeachType!R) : Iterator!E {
	mixin Ctor!R;

	@property override E front() => a.front;

	@property override bool empty() {
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

	override void popFront() {
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
	=> scoped!(Intersect!R)(a, b);

unittest {
	import std.algorithm;
	import std.range;

	auto e = intersect(iota(0, 0), iota(2, 5));
	assert(e.empty);
	assert(intersect(iota(0, 10), iota(5, 15)).equal(iota(5, 10)));
	auto y = intersect(iota(0, 10, 2), iota(1, 10, 2));
	assert(y.empty);
}

class Union(R, E = ForeachType!R) : Iterator!E {
	mixin Ctor!R;

	@property override E front() {
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

	@property override bool empty() => a.empty && b.empty;

	override void popFront() {
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
	=> scoped!(Union!R)(a, b);

unittest {
	import std.algorithm;
	import std.range;

	assert(union_(iota(0, 0), iota(2, 5)).equal(iota(2, 5)));
	assert(union_(iota(0, 10), iota(5, 15)).equal(iota(0, 15)));
	assert(union_(iota(0, 10, 2), iota(1, 10, 2)).equal(iota(0, 10, 1)));
}

class Diff(R, E = ForeachType!R) : Iterator!E {
	mixin Ctor!R;

	@property override E front() => a.front;

	@property override bool empty() {
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

	override void popFront() {
		if (!a.empty) {
			if (b.empty)
				a.popFront();
			else
				pop(a, b);
		}
	}
}

auto diff(R)(R a, R b)
	=> scoped!(Diff!R)(a, b);

unittest {
	import std.algorithm;
	import std.range;

	assert(diff(iota(0, 0), iota(2, 5)).equal(iota(0, 0)));
	assert(diff(iota(0, 10), iota(5, 15)).equal(iota(0, 5)));
	assert(diff(iota(0, 10, 2), iota(1, 10, 2)).equal(iota(0, 10, 2)));
}

class SymDiff(R, E = ForeachType!R) : Iterator!E {
	mixin Ctor!R;

	@property override E front() {
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

	@property override bool empty() {
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

	override void popFront() {
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
	=> scoped!(SymDiff!R)(a, b);

unittest {
	import std.algorithm;
	import std.range;

	assert(symDiff(iota(0, 0), iota(2, 5)).equal(iota(2, 5)));
	assert(symDiff(iota(0, 10), iota(5, 15)).equal([
		0, 1, 2, 3, 4, 10, 11, 12, 13, 14
	]));
	assert(symDiff(iota(0, 10, 2), iota(1, 10, 2)).equal(iota(0, 10, 1)));
}

class Ordered(R, bool allowDup = true, E = ForeachType!R) : Iterator!E {
	import std.container.rbtree;
	import std.range;

	alias RBTree = RedBlackTree!(E, less, allowDup);
	alias Less = extern (D) bool function(E, E);
	Less less;
	private {
		R r;
		Take!(RBTree.Range) tree;
	}

	this(R range, size_t limit = size_t.max) {
		this(range, (E a, E b) => a < b, limit);
	}

	this(R range, Less cmp, size_t limit = size_t.max) {
		r = range;
		less = cmp;
		auto t = new RBTree(r);
		tree = t[].take(limit);
	}

	@property override E front()
	in (!empty) => tree.front;

	@property override bool empty() => tree.empty;

	override void popFront() => tree.popFront();
}

auto ordered(R, bool allowDup = true, E = ForeachType!R)(R r, size_t limit = size_t.max)
	=> scoped!(Ordered!(R, allowDup, E))(r, limit);

extern (D) auto ordered(R, bool allowDup = true, E = ForeachType!R)(R r, bool function(E, E) cmp, size_t limit = size_t
		.max)
	=> scoped!(Ordered!(R, allowDup, E))(r, cmp, limit);

unittest {
	import std.algorithm;
	import std.range;

	auto r = iota(0, 0);
	auto o = ordered(r);
	assert(o.empty);
}

unittest {
	import std.algorithm;
	import std.range;

	static struct TestRange {
		int i;
		@property bool empty() => i == 5;

		@property int front() {
			switch (i) {
			case 0:
				return 1;
			case 1:
				return 4;
			case 2:
				return 2;
			case 3:
				return 3;
			case 4:
				return 5;
			default:
				assert(0);
			}
		}

		void popFront() {
			++i;
		}
	}

	auto r = TestRange();
	assert(ordered(r).equal([1, 2, 3, 4, 5]));
	assert(ordered(r, 3).equal([1, 2, 3]));
	assert(ordered(r, 10).equal([1, 2, 3, 4, 5]));
	auto greater = (int a, int b) => b < a;
	assert(ordered!TestRange(r, greater).equal([5, 4, 3, 2, 1]));
	assert(ordered!TestRange(r, greater, 3).equal([5, 4, 3]));
}
