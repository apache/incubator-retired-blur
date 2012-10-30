package com.nearinfinity.agent.exceptions;

@SuppressWarnings("serial")
public class ZookeeperNameMissingException extends MissingException {
	public ZookeeperNameMissingException(String zookeeperName) {
		super("Zookeeper", zookeeperName);
	}
}
