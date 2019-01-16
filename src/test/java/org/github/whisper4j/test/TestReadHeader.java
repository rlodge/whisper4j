package org.github.whisper4j.test;

import org.github.whisper4j.AggregationMethod;
import org.github.whisper4j.ArchiveInfo;
import org.github.whisper4j.Header;
import org.github.whisper4j.Point;
import org.github.whisper4j.TimeInfo;
import org.github.whisper4j.Whisper;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.ToLongFunction;

public class TestReadHeader {
	public static String getWhistperFile(Class anyTestClass, String fileName) {
		final String clsUri = anyTestClass.getName().replace('.', '/')
				+ ".class";
		final URL url = anyTestClass.getClassLoader().getResource(clsUri);
		String clsPath = url.getPath();
		clsPath = clsPath.replace("%20", " ");
		final File root = new File(clsPath.substring(0, clsPath.length()
				- clsUri.length()));
		String str = root.getAbsolutePath() + File.separator + fileName;
		return str;
	}

	@Test
	public void testAnalyticsWhisperFile() throws Exception {
		String testFile = new File(System.getProperty("user.dir"), "target/test-classes/p50.wsp").getAbsolutePath();
		System.out.println(testFile);
		Whisper jisper = new Whisper();
		Header header = jisper.info(testFile);
		Assert.assertNotNull(header);
		Assert.assertNotNull(header.metadata);
		Assert.assertEquals(AggregationMethod.Average.getIntValue(), header.metadata.aggregationType);
		Assert.assertEquals("archiveCount", 1, header.metadata.archiveCount);
		Assert.assertEquals("xFileFactor", 0.5f, header.metadata.xFileFactor, 0.001);
		Assert.assertEquals("maxRetention", 10800000, header.metadata.maxRetention);
		Assert.assertNotNull(header.archiveInfo);
		Assert.assertEquals(1, header.archiveInfo.size());
		ArchiveInfo info = header.archiveInfo.get(0);
		Assert.assertEquals("Points", 180000, info.points);
		Assert.assertEquals("secondsPerPoint", 60, info.secondsPerPoint);
		Assert.assertEquals("offset", 28, info.offset);
		Assert.assertEquals("size", 2160000, info.size);
		Assert.assertEquals("retention", 10800000, info.retention);
		TimeInfo result = jisper.fetch(testFile, Integer.MIN_VALUE, Integer.MAX_VALUE);
		Assert.assertEquals("Points", 180000, result.points.length);
		final List<Point> points = Arrays.asList(result.points);
		points.sort(Comparator.comparingLong((ToLongFunction<Point>) value -> value.timestamp).reversed());
		Optional<Point> maybePoint = points.stream().filter(p -> p.timestamp == 1547232300L)
			.findFirst();

		if (maybePoint.isPresent()) {
			final Point point = maybePoint.get();
			Assert.assertEquals("Point value", 1084370F, point.value, 0.000001f);
			Assert.assertEquals("Point value should be whole number", 0, point.value % 1, 0.0);
		} else {
			Assert.fail("Must get point for 1547232300");
		}
		
		// points.subList(0, 500).forEach(p -> System.out.println(p.timestamp + " " + new Date(p.timestamp * 1000L) + " => " + p.value));
	}

	@Test
	public void testAnalyticsProdWhisperFile() throws Exception {
		String testFile = new File(System.getProperty("user.dir"), "target/test-classes/p99.wsp").getAbsolutePath();
		System.out.println(testFile);
		Whisper jisper = new Whisper();
		Header header = jisper.info(testFile);
		Assert.assertNotNull(header);
		Assert.assertNotNull(header.metadata);
		Assert.assertEquals(AggregationMethod.Average.getIntValue(), header.metadata.aggregationType);
		Assert.assertEquals("archiveCount", 1, header.metadata.archiveCount);
		Assert.assertEquals("xFileFactor", 0.5f, header.metadata.xFileFactor, 0.001);
		Assert.assertEquals("maxRetention", 34387200, header.metadata.maxRetention);
		Assert.assertNotNull(header.archiveInfo);
		Assert.assertEquals(1, header.archiveInfo.size());
		ArchiveInfo info = header.archiveInfo.get(0);
		Assert.assertEquals("Points", 573120, info.points);
		Assert.assertEquals("secondsPerPoint", 60, info.secondsPerPoint);
		Assert.assertEquals("offset", 28, info.offset);
		Assert.assertEquals("size", 6877440, info.size);
		Assert.assertEquals("retention", 34387200, info.retention);
		long start = System.nanoTime();
		TimeInfo result = jisper.fetch(testFile, Integer.MIN_VALUE, Integer.MAX_VALUE);
		long loaded = System.nanoTime();
		final List<Point> points = Arrays.asList(result.points);
		points.sort(Comparator.comparingLong((ToLongFunction<Point>) value -> value.timestamp).reversed());
		long sorted = System.nanoTime();
//		System.out.println("Loaded in " + TimeUnit.NANOSECONDS.toMillis(loaded - start) + "ms; sorted in " + TimeUnit.NANOSECONDS.toMillis(sorted - loaded) + "ms");
		Assert.assertEquals("Points", 573120, result.points.length);
		Optional<Point> maybePoint = points.stream().filter(p -> p.timestamp == 1547232300L)
			.findFirst();

		if (maybePoint.isPresent()) {
			final Point point = maybePoint.get();
			Assert.assertEquals("Point value", 8329421F, point.value, 0.000001f);
			Assert.assertEquals("Point value should be whole number", 0, point.value % 1, 0.0);
		} else {
			Assert.fail("Must get point for 1547232300");
		}

		// points.subList(0, 500).forEach(p -> System.out.println(p.timestamp + " " + new Date(p.timestamp * 1000L) + " => " + p.value));
	}

	@Test
	public void test_60_1440() throws Exception {
		String testFile = getWhistperFile(getClass(), "60_1440.wsp");

		Util.delete(testFile);
		Util.create(testFile, "60:1440");

		Whisper jisper = new Whisper();
		Header header = jisper.info(testFile);
		Assert.assertNotNull(header);
		Assert.assertNotNull(header.metadata);
		Assert.assertEquals(AggregationMethod.Average.getIntValue(),
				header.metadata.aggregationType);
		Assert.assertEquals("archiveCount", 1, header.metadata.archiveCount);
		Assert.assertEquals("maxRetention", 86400, header.metadata.maxRetention);
		Assert.assertEquals("xFileFactor", 0.5f, header.metadata.xFileFactor,
				0.001);

		Assert.assertNotNull(header.archiveInfo);
		Assert.assertEquals(1, header.archiveInfo.size());

		ArchiveInfo info = header.archiveInfo.get(0);
		Assert.assertEquals("Points", 1440, info.points);
		Assert.assertEquals("secondsPerPoint", 60, info.secondsPerPoint);
		Assert.assertEquals("offset", 28, info.offset);
		Assert.assertEquals("size", 17280, info.size);
		Assert.assertEquals("retention", 86400, info.retention);
	}

	@Test
	public void test_12h_2y() throws Exception {
		String testFile = getWhistperFile(getClass(), "12h_2y.wsp");
		Util.delete(testFile);
		Util.create(testFile, "12h:2y");
		
		Whisper jisper = new Whisper();
		Header header = jisper.info(testFile);
		Assert.assertNotNull(header);
		Assert.assertNotNull(header.metadata);
		Assert.assertEquals(AggregationMethod.Average.getIntValue(),
				header.metadata.aggregationType);
		Assert.assertEquals("archiveCount", 1, header.metadata.archiveCount);
		Assert.assertEquals("maxRetention", 63072000,
				header.metadata.maxRetention);
		Assert.assertEquals("xFileFactor", 0.5f, header.metadata.xFileFactor,
				0.001);

		Assert.assertNotNull(header.archiveInfo);
		Assert.assertEquals(1, header.archiveInfo.size());

		ArchiveInfo info = header.archiveInfo.get(0);
		Assert.assertEquals("Points", 1460, info.points);
		Assert.assertEquals("secondsPerPoint", 43200, info.secondsPerPoint);
		Assert.assertEquals("offset", 28, info.offset);
		Assert.assertEquals("size", 17520, info.size);
		Assert.assertEquals("retention", 63072000, info.retention);
	}

	@Test
	public void test_15m_8() throws Exception {
		String testFile = getWhistperFile(getClass(), "15m_8.wsp");
		Util.delete(testFile);
		Util.create(testFile, "15m:8");


		Whisper jisper = new Whisper();
		Header header = jisper.info(testFile);
		Assert.assertNotNull(header);
		Assert.assertNotNull(header.metadata);
		Assert.assertEquals(AggregationMethod.Average.getIntValue(),
				header.metadata.aggregationType);
		Assert.assertEquals("archiveCount", 1, header.metadata.archiveCount);
		Assert.assertEquals("maxRetention", 7200, header.metadata.maxRetention);
		Assert.assertEquals("xFileFactor", 0.5f, header.metadata.xFileFactor,
				0.001);

		Assert.assertNotNull(header.archiveInfo);
		Assert.assertEquals(1, header.archiveInfo.size());

		ArchiveInfo info = header.archiveInfo.get(0);
		Assert.assertEquals("Points", 8, info.points);
		Assert.assertEquals("secondsPerPoint", 900, info.secondsPerPoint);
		Assert.assertEquals("offset", 28, info.offset);
		Assert.assertEquals("size", 96, info.size);
		Assert.assertEquals("retention", 7200, info.retention);
	}

	@Test
	public void testFetch() throws Exception {
		String testFile = getWhistperFile(getClass(), "out.wsp");

		Whisper jisper = new Whisper();
		Header header = jisper.info(testFile);
		Assert.assertNotNull(header);
		System.out.println(header.toString());
		Assert.assertNotNull(header.metadata);
		Assert.assertEquals(AggregationMethod.Average.getIntValue(),
				header.metadata.aggregationType);
		Assert.assertEquals("archiveCount", 1, header.metadata.archiveCount);
		Assert.assertEquals("maxRetention", 86400, header.metadata.maxRetention);
		Assert.assertEquals("xFileFactor", 0.5f, header.metadata.xFileFactor,
				0.001);

		Assert.assertNotNull(header.archiveInfo);
		Assert.assertEquals(1, header.archiveInfo.size());

		ArchiveInfo info = header.archiveInfo.get(0);
		Assert.assertEquals("Points", 1440, info.points);
		Assert.assertEquals("secondsPerPoint", 60, info.secondsPerPoint);
		Assert.assertEquals("offset", 28, info.offset);
		Assert.assertEquals("size", 17280, info.size);
		Assert.assertEquals("retention", 86400, info.retention);

		// TimeInfo result = jisper.fetch(testFile, (int)1313585674,
		// 1313672074);
		TimeInfo result = jisper.fetch(testFile, Integer.MIN_VALUE,
				Integer.MAX_VALUE);

		// 1313659560 1.000000
		// 1313659620 None
		// ...
		// 1313660400 None
		// 1313660460 None
		// 1313660520 None
		// 1313660580 2.000000
		Assert.assertNotNull(result);
		Assert.assertNotNull(result.points);
		Assert.assertEquals(
				"points read is not the same as what archiveinfo says",
				info.points, result.points.length);

		int validPoints = 0;
		for (Point point : result.points) {
			if (point != null && point.timestamp == 1313659560) {
				Assert.assertEquals(1.0f, point.value, 0.00001);
				validPoints++;
			}
			if (point != null && point.timestamp == 1313660580) {
				Assert.assertEquals(2.0f, point.value, 0.00001);
				validPoints++;
			}
		}
		// Assert.assertEquals(2,validPoints);
	}

}
