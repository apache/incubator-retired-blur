package com.nearinfinity.license;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import com.nearinfinity.agent.exceptions.InvalidLicenseException;
import com.nearinfinity.agent.monitor.ThreadController;
import com.nearinfinity.license.utils.CryptoServices;
import com.nearinfinity.license.utils.CryptoServicesException;
import com.nearinfinity.license.utils.IssuingKey;

public class AgentLicense {
	private static final Log log = LogFactory.getLog(AgentLicense.class);
	private static final String PUBLIC_KEY = "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAr70Zcg1WlzllR6xuxJEBuaETSbgAHY6IuZ9lqLWPEdj1I8cQdk28NONmsqMj0jHBjDmqby1YYIsj3+kxib6Ow2rxL/r2EXsdzZxXTdVCDzJMX1JqVSKLqXmbYB7gy9xLqxxDYCVZwTmhAQmhU1KN1PA6IzvyNFcPfG11qyzrfwqAJ+8rA4556KtEe9EbWBPh3PEEqcuq7lQAFXrrr32cvluSVgqgTF2eLYaRyyPxgvBTkFSg4OkdzT2gP+ggiL9SBXA9y6yLZZR2ehMxS5GhS+96n95wLF5CIbg4onefQzZ65/VGwaSeWbPM/8RBUcK/jRC99rNcF6phywJCiKMgJ+u9cj1ThuIBm5LhSs2FreCuDNtUkna4ernk0Ndy1vSpTu0+h5IiMbz2cbfDJq++w8Os7J/Pt3XfgON2cHcJyEEbkzsJ71Z8+lebMydzX/al95FyXDrobuUuUfOVNrXWKqFB+BRIHPTVJo2tP9FGjcxhmj5vHQkGEAh/0n9b93cCBN6e//ft9TP/yELT+lSFJIeJOHj8gUvSFT5JfilTjZLHvLweMR9ZVlfseq1NC8EoYWgpa6PQcUh2PzuTBgoCyRCG4/mIqw45ICuLeS/MwGERJqWpxWPKRpSbklWDrFObOrfaPb+bvKzsPvbf0Q1x+Azqr9d4OSm7DunNAtV85aUCAwEAAQ==";
	private static final String PRIVATE_KEY = "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";

	public static void verifyLicense(Properties props, JdbcTemplate jdbc) throws InvalidLicenseException {
		List<String> licenseFileLines = readLicenseFile(props);
		String licenseType = licenseFileLines.get(3);
		verifyLicenseIntegrity(licenseFileLines, licenseType);
		verifyLicenseValidity(licenseFileLines, licenseType, jdbc, props);
		System.out.println(licenseType);
		if ("NODE_YEARLY".equals(licenseType)) {
			monitorNodeCount(licenseFileLines, props, jdbc);
		} else if ("CLUSTER_YEARLY".equals(licenseType)) {
			monitorClusterCount(licenseFileLines, jdbc);
		}
	}

	private static List<String> readLicenseFile(Properties props) throws InvalidLicenseException {
		String licenseFilePath = props.getProperty("license.file");

		if (StringUtils.isBlank(licenseFilePath)) {
			throw new InvalidLicenseException("Missing license.file configuration property.");
		}

		File licenseFile = new File(licenseFilePath);
		List<String> licenseFileLines = null;
		try {
			licenseFileLines = IOUtils.readLines(new FileInputStream(licenseFile));
		} catch (FileNotFoundException e) {
			throw new InvalidLicenseException("Unable to find license file (" + licenseFile.getAbsolutePath() + ").");
		} catch (IOException e) {
			throw new InvalidLicenseException("There was a problem reading the license file: " + e.getMessage() + ".");
		}
		return licenseFileLines;
	}

	private static void verifyLicenseIntegrity(List<String> licenseFileLines, String licenseType) throws InvalidLicenseException {
		String licenseData = null;
		String signature = null;
		if ("YEARLY".equals(licenseType)) {
			licenseData = StringUtils.join(licenseFileLines.subList(1, 7), "\n");
			signature = StringUtils.join(licenseFileLines.subList(7, licenseFileLines.size() - 1), "\n");
		} else if ("NODE_YEARLY".equals(licenseType)) {
			licenseData = StringUtils.join(licenseFileLines.subList(1, 8), "\n");
			signature = StringUtils.join(licenseFileLines.subList(8, licenseFileLines.size() - 1), "\n");
		} else if ("CLUSTER_YEARLY".equals(licenseType)) {
			licenseData = StringUtils.join(licenseFileLines.subList(1, 8), "\n");
			signature = StringUtils.join(licenseFileLines.subList(8, licenseFileLines.size() - 1), "\n");
		} else {
			throw new InvalidLicenseException("Invalid license type [" + licenseType + "].");
		}

		CryptoServices cryptoServices = CryptoServices.getCryptoServices();
		IssuingKey issuingKey = new IssuingKey();
		issuingKey.setDescription("Main");
		issuingKey.setPrivateKey(CryptoServices.getCryptoServices().decodeBase64(PRIVATE_KEY));
		issuingKey.setPublicKey(CryptoServices.getCryptoServices().decodeBase64(PUBLIC_KEY));

		try {
			if (!cryptoServices.verify(licenseData.toString().getBytes(), cryptoServices.decodeBase64(signature), cryptoServices.getPublicKey(issuingKey.getPublicKey()))) {
				throw new InvalidLicenseException("Invalid license.");
			}
		} catch (CryptoServicesException e) {
			throw new InvalidLicenseException("There was a problem decrypting license.");
		}
	}

	private static void verifyLicenseValidity(List<String> licenseFileLines, String licenseType, JdbcTemplate jdbc,	Properties props) throws InvalidLicenseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Date expires = null;
		try {
			expires = sdf.parse(licenseFileLines.get(5));
		} catch (ParseException e) {
			throw new InvalidLicenseException("Unable to parse expires date.", e);
		}

		if (StringUtils.endsWith(licenseType, "YEARLY") && expires.getTime() < System.currentTimeMillis()) {
			throw new InvalidLicenseException("License has expired.");
		}

		jdbc.update("delete from licenses");
		try {
			jdbc.update(
					"insert into licenses (org, issued_date, expires_date, node_overage, grace_period_days_remain, cluster_overage) values (?,?,?,?,?,?)",
					licenseFileLines.get(1), sdf.parse(licenseFileLines.get(4)), expires, 0, 60, 0);
		} catch (Exception e) {
			throw new InvalidLicenseException("Unable to insert license into DB.", e);
		}

		if ("NODE_YEARLY".equals(licenseType)) {
			updateNodeOverageInfo(jdbc, licenseFileLines, props);
		} else if ("CLUSTER_YEARLY".equals(licenseType)) {
			updateClusterOverageInfo(jdbc, licenseFileLines);
		}
	}

	private static void monitorNodeCount(final List<String> licenseFileLines, final Properties props, final JdbcTemplate jdbc) {
		Thread monitorThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(TimeUnit.HOURS.toMillis(1));
					} catch (InterruptedException e) {
						break;
					}
					try {
						updateNodeOverageInfo(jdbc, licenseFileLines, props);
					} catch (InvalidLicenseException e) {
						log.fatal(e.getMessage() + " Stopping node count monitor.");
						ThreadController.stopAllThreads();
						break;
					}
				}
			}
		}, "Node Count Monitor");
		monitorThread.start();
		ThreadController.registerThread(monitorThread);
	}

	private static void monitorClusterCount(final List<String> licenseFileLines, final JdbcTemplate jdbc) {
		Thread monitorThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(TimeUnit.HOURS.toMillis(1));
					} catch (InterruptedException e) {
						break;
					}
					try {
						updateClusterOverageInfo(jdbc, licenseFileLines);
					} catch (InvalidLicenseException e) {
						log.fatal(e.getMessage() + " Stopping cluster count monitor.");
						ThreadController.stopAllThreads();
						break;
					}
				}
			}
		}, "Cluster Count Monitor");
		monitorThread.start();
		ThreadController.registerThread(monitorThread);
	}

	private static void updateClusterOverageInfo(JdbcTemplate jdbc, List<String> licenseFileLines) throws InvalidLicenseException {
		int totalClusters = jdbc.queryForInt("select count(1) from clusters");
		int allowedClusters = Integer.parseInt(licenseFileLines.get(7));

		if (totalClusters > allowedClusters) {
			jdbc.update("update licenses set cluster_overage=?", totalClusters - allowedClusters);
			throw new InvalidLicenseException(
					"Too many clusters for license.  Request new license to continue collecting information.");
		}
	}

	private static void updateNodeOverageInfo(JdbcTemplate jdbc, List<String> licenseFileLines, Properties props) throws InvalidLicenseException {
		int totalNodes = jdbc
				.queryForInt("select a.nodes + b.nodes from (select count(1) as nodes from controllers) a, (select count(1) as nodes from shards) b");
		int allowedNodes = Integer.parseInt(licenseFileLines.get(7));
		Set<String> dates = loadGracePeriodDates(props);

		if (totalNodes > allowedNodes) {
			Set<String> newDates = new HashSet<String>(dates);
			newDates.add(new SimpleDateFormat("yyyyMMdd").format(new Date()));

			int overage = totalNodes > allowedNodes ? totalNodes - allowedNodes : 0;
			int graceDays = 60 - dates.size();
			jdbc.update("update licenses set node_overage=?, grace_period_days_remain=?", overage, graceDays);

			if (newDates.size() > dates.size()) {
				writeGracePeriodDates(props, newDates);
			}

			if (newDates.size() > 60) {
				throw new InvalidLicenseException(
						"Too many nodes for license and 60 day grace period is up.  Request new license to continue collecting information.");
			}
		}
	}

	private static void writeGracePeriodDates(Properties props, Set<String> dates) throws InvalidLicenseException {
		String licenseFilePath = props.getProperty("license.file");
		File licenseFile = new File(licenseFilePath);

		File graceFile = new File(licenseFile.getParentFile(), licenseFile.getName().replace(".lic", ".grc"));

		CryptoServices cryptoServices = CryptoServices.getCryptoServices();
		IssuingKey issuingKey = new IssuingKey();
		issuingKey.setDescription("Main");
		issuingKey.setPrivateKey(CryptoServices.getCryptoServices().decodeBase64(PRIVATE_KEY));
		issuingKey.setPublicKey(CryptoServices.getCryptoServices().decodeBase64(PUBLIC_KEY));

		byte[] sig = null;
		try {
			sig = cryptoServices.sign(StringUtils.join(dates, ',').getBytes(), cryptoServices.getPrivateKey(issuingKey.getPrivateKey()));
		} catch (CryptoServicesException e) {
			throw new InvalidLicenseException("Unable to sign grace file contents.", e);
		}
		String fileContent = StringUtils.join(dates, ',') + "\n"
				+ WordUtils.wrap(cryptoServices.encodeBase64(sig), 50, null, true);

		try {
			IOUtils.write(fileContent, new FileOutputStream(graceFile));
		} catch (Exception e) {
			throw new InvalidLicenseException("Unable to write grace file.", e);
		}
	}

	private static Set<String> loadGracePeriodDates(Properties props) throws InvalidLicenseException {
		String licenseFilePath = props.getProperty("license.file");
		File licenseFile = new File(licenseFilePath);

		File graceFile = new File(licenseFile.getParentFile(), licenseFile.getName().replace(".lic", ".grc"));
		List<String> graceFileLines = null;
		try {
			graceFileLines = IOUtils.readLines(new FileInputStream(graceFile));
		} catch (FileNotFoundException e) {
			throw new InvalidLicenseException("Unable to find grace period file (" + graceFile.getAbsolutePath() + ").");
		} catch (IOException e) {
			throw new InvalidLicenseException("There was a problem reading the grace period file: " + e.getMessage()
					+ ".");
		}

		if (graceFileLines.isEmpty()) {
			throw new InvalidLicenseException("Grace period file can not be empty.");
		}

		CryptoServices cryptoServices = CryptoServices.getCryptoServices();
		IssuingKey issuingKey = new IssuingKey();
		issuingKey.setDescription("Main");
		issuingKey.setPrivateKey(CryptoServices.getCryptoServices().decodeBase64(PRIVATE_KEY));
		issuingKey.setPublicKey(CryptoServices.getCryptoServices().decodeBase64(PUBLIC_KEY));

		String dates = graceFileLines.get(0);
		String signature = StringUtils.join(graceFileLines.subList(1, graceFileLines.size()), "\n");
		try {
			if (!cryptoServices.verify(dates.getBytes(), cryptoServices.decodeBase64(signature),
					cryptoServices.getPublicKey(issuingKey.getPublicKey()))) {
				throw new InvalidLicenseException("Invalid grace period file.");
			}
		} catch (CryptoServicesException e) {
			throw new InvalidLicenseException("There was a problem decrypting grace period file.");
		}

		return new HashSet<String>(Arrays.asList(StringUtils.split(dates, ",")));
	}
}
