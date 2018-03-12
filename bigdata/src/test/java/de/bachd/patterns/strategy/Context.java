package de.bachd.patterns.strategy;

public class Context {
	private Strategy strategy = null;

	public void setStrategy(Strategy strat) {
		this.strategy = strat;
	}

	public void execute() {
		if (strategy != null) {
			strategy.algorithm();
		}
	}
}
