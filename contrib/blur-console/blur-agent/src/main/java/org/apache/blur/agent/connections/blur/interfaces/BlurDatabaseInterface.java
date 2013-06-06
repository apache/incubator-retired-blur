package org.apache.blur.agent.connections.blur.interfaces;

import java.util.List;
import java.util.Map;

import org.apache.blur.agent.connections.blur.interfaces.TableDatabaseInterface;
import org.apache.blur.agent.exceptions.TableCollisionException;
import org.apache.blur.agent.exceptions.TableMissingException;
import org.apache.blur.agent.exceptions.ZookeeperNameCollisionException;
import org.apache.blur.agent.exceptions.ZookeeperNameMissingException;


public interface BlurDatabaseInterface extends TableDatabaseInterface, QueryDatabaseInterface {
	String resolveConnectionString(int zookeeperId);

	String getZookeeperId(final String zookeeperName) throws ZookeeperNameMissingException, ZookeeperNameCollisionException;

	List<Map<String, Object>> getClusters(final int zookeeperId);

	Map<String, Object> getExistingTable(final String table, final Integer clusterId) throws TableMissingException, TableCollisionException;

}
