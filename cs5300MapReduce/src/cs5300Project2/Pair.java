package cs5300Project2.blocked;

public class Pair<L, R> {
	
	private L left;
	private R right;
	
	public Pair (L l, R r) {
		left = l;
		right = r;
	}
	
	public L left () {
		return left;
	}
	
	public R right () {
		return right;
	}
}
