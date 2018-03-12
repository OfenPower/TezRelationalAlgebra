package de.bachd.patterns.strategy;

public class Client {
	public static void main(String[] args) {
		Context context = new Context();

		context.setStrategy(new StrategyImplementationA());
		context.execute();
		context.setStrategy(new StrategyImplementationB());
		context.execute();
	}
}
