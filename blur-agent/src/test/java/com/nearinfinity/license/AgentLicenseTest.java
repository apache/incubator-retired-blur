package com.nearinfinity.license;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import com.nearinfinity.BlurAgentBaseTestClass;
import com.nearinfinity.agent.exceptions.InvalidLicenseException;
import com.nearinfinity.agent.monitor.ThreadController;

public class AgentLicenseTest extends BlurAgentBaseTestClass {
	@Before
	public void setup() {
		ThreadController.exitOnStop = false;
	}

	@Test
	public void testMissingLicenseLocation() {
		try {
			AgentLicense.verifyLicense(new Properties(), jdbc);
			fail("Missing file, this should have failed");
		} catch (InvalidLicenseException e) {
			assertEquals("Missing license.file configuration property.", e.getMessage());
		}
	}

	@Test
	public void testMissingLicenseFile() {
		try {
			Properties props = new Properties();
			props.setProperty("license.file", new File("bad.lic").getAbsolutePath());
			AgentLicense.verifyLicense(props, jdbc);
			fail("Missing file, this should have failed");
		} catch (InvalidLicenseException e) {
			assertEquals("Unable to find license file (" + new File("bad.lic").getAbsolutePath() + ").", e.getMessage());
		}
	}

	@Test
	public void testBadLicenseType() {
		try {
			Properties props = new Properties();
			props.setProperty("license.file", getLicensePath("./invalid_type.lic"));
			AgentLicense.verifyLicense(props, jdbc);
			fail("Missing file, this should have failed");
		} catch (InvalidLicenseException e) {
			assertEquals("Invalid license type [UNKNOWN].", e.getMessage());
		}
	}

	@Test
	public void testInvalidLicense() {
		try {
			Properties props = new Properties();
			props.setProperty("license.file", getLicensePath("./invalid_matching.lic"));
			AgentLicense.verifyLicense(props, jdbc);
			fail("Missing file, this should have failed");
		} catch (InvalidLicenseException e) {
			assertEquals("Invalid license.", e.getMessage());
		}
	}

//	@Test
//	public void testInvalidLicenseType() throws InvalidLicenseException {
//		Properties props = new Properties();
//		props.setProperty("license.file", "/Users/crohr/Projects/nic/blur-tools/license-maker/licenses/NIC-Testing/blur_tools_2011-12-31.lic");
//
//		AgentLicense.verifyLicense(props, jdbc);
//		// AgentLicense.verifyLicense(null, null);
//	}

	private String getLicensePath(String license) {
		String file = null;
		try {
			file = new ClassPathResource(license).getFile().getAbsolutePath();
		} catch (IOException e) {
			fail(e.getMessage());
		}
		return file;
	}
}
