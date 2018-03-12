package de.bachd.bigdata.utils;

public class Pair<L, R> {
	private L l;
	private R r;

	public Pair(L l, R r) {
		this.l = l;
		this.r = r;
	}

	public L getLeft() {
		return this.l;
	}

	public R getRight() {
		return this.r;
	}

	public void setLeft(L l) {
		this.l = l;
	}

	public void setRight(R r) {
		this.r = r;
	}
}
