package com.ctrip.hermes.metaservice.service;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.reflect.AvroDefault;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.codehaus.jackson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.env.ClientEnvironment;

@Named
public class CompileService {

	private static final Logger logger = LoggerFactory.getLogger(CompileService.class);

	private JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

	private static final String RELEASE_REPO = "http://maven.dev.sh.ctripcorp.com:8081/nexus/content/repositories/fxrelease";

	private static final String SNAPSHORT_REPO = "http://maven.dev.sh.ctripcorp.com:8081/nexus/content/repositories/fxsnapshot";

	@Inject
	private ClientEnvironment m_env;

	/**
	 * 
	 * @param destDir
	 * @throws IOException
	 */
	public void compile(final Path destDir) throws IOException {
		logger.debug("compile destDir {}", destDir.getFileName());
		final List<File> files = new ArrayList<File>();
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				if (file.toString().endsWith(".java"))
					files.add(file.toFile());
				return super.visitFile(file, attrs);
			}

		});

		StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
		Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromFiles(files);
		List<String> options = new ArrayList<String>();
		options.add("-source");
		options.add("1.7");
		options.add("-target");
		options.add("1.7");

		String avro = AvroDefault.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		String avroc = SpecificCompiler.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		String json = JsonParser.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		options.add("-classpath");
		options.add(String.format("%s:%s:%s:%s", System.getProperty("java.class.path"), avro, avroc, json));

		compiler.getTask(null, fileManager, diagnostics, options, null, compilationUnits).call();
		for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics())
			logger.warn(String.format("%s on line %d in %s: %s%n", diagnostic.getKind().toString(),
			      diagnostic.getLineNumber(), diagnostic.getSource() != null ? diagnostic.getSource().toUri() : "",
			      diagnostic.getMessage(null)));
		fileManager.close();
	}

	/**
	 * 
	 * @param destDir
	 * @throws IOException
	 */
	public void delete(final Path destDir) throws IOException {
		logger.debug("delete path {}", destDir);
		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return super.postVisitDirectory(dir, exc);
			}

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				Files.delete(path);
				return super.visitFile(path, attrs);
			}

		});
	}

	public void deployToMaven(Path jarPath, String groupId, String artifactId, String version, String repositoryId)
	      throws Exception {
		String cmd = "mvn";
		StringBuilder sb = new StringBuilder();
		sb.append("deploy:deploy-file ").append("-DgroupId=").append(groupId).append(" ");
		sb.append("-DartifactId=").append(artifactId).append(" ");
		sb.append("-Dfile=").append(jarPath.toAbsolutePath().toString()).append(" ");
		if ("snapshots".equals(repositoryId)) {
			sb.append("-Durl=").append(SNAPSHORT_REPO).append(" ");
			sb.append("-DrepositoryId=").append(repositoryId).append(" ");
			if (!version.endsWith("SNAPSHOT")) {
				version = version + "-SNAPSHOT";
			}
		} else if ("releases".equals(repositoryId)) {
			sb.append("-Durl=").append(RELEASE_REPO).append(" ");
			sb.append("-DrepositoryId=").append(repositoryId).append(" ");
		}
		sb.append("-Dversion=").append(version).append(" ");

		String command = sb.toString();
		logger.debug("deploy to maven {}", command);
		Pair<Boolean, Pair<String, String>> result = executeCommand(cmd, sb.toString().split(" "), null);
		if (!result.getKey()) {
			throw new RuntimeException("Compile schema to .cs file failed: " + result.getValue().getValue());
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void compileSchemaToCs(File schemaFile, Path destDir) throws Exception {
		String cmd = "mono";
		String[] arguments = "${avrogen} -s ${schemaFile} ${destDir}".split(" ");
		Map substitutionMap = new HashMap<>();
		substitutionMap.put("avrogen", new File(m_env.getGlobalConfig().getProperty("portal.avrogen.path")));
		substitutionMap.put("schemaFile", schemaFile);
		substitutionMap.put("destDir", destDir);
		Pair<Boolean, Pair<String, String>> result = executeCommand(cmd, arguments, substitutionMap);
		if (!result.getKey()) {
			throw new RuntimeException("Compile schema to .cs file failed: " + result.getValue().getValue());
		}
		if (destDir.toFile().list().length == 0) {
			throw new RuntimeException("Compile schema to .cs file failed: " + result.getValue().getKey());
		}
	}

	/**
	 * 
	 * @param cmd
	 * @param arguments
	 *           If the argument doesn't include spaces or quotes, return it as is. If it contains double quotes, use single quotes -
	 *           else surround the argument by double quotes.
	 * @param substitutionMap
	 * @return Return.key indicates whether the process success or not. Return.value stores the normal output and err output msgs.
	 * @throws Exception
	 */

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Pair<Boolean, Pair<String, String>> executeCommand(String cmd, String[] arguments, Map substitutionMap)
	      throws Exception {
		String osName = System.getProperty("os.name").toLowerCase(Locale.US);
		String[] windowsArguments = null;
		if (osName.contains("windows")) {
			windowsArguments = new String[] { "/c", cmd };
			cmd = "cmd";
		}
		CommandLine cmdLine = new CommandLine(cmd);

		if (windowsArguments != null && windowsArguments.length != 0) {
			for (String argument : windowsArguments) {
				cmdLine.addArgument(argument);
			}
		}

		if (arguments != null && arguments.length != 0) {
			for (String argument : arguments) {
				cmdLine.addArgument(argument);
			}
		}
		if (substitutionMap != null && !substitutionMap.isEmpty())
			cmdLine.setSubstitutionMap(substitutionMap);

		ExecuteWatchdog watchdog = new ExecuteWatchdog(60 * 1000);
		Executor executor = new DefaultExecutor();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteArrayOutputStream err = new ByteArrayOutputStream();

		executor.setStreamHandler(new PumpStreamHandler(out, err));
		executor.setWatchdog(watchdog);

		int exitValue = 0;
		exitValue = executor.execute(cmdLine);
		if (watchdog.killedProcess()) {
			throw new RuntimeException("Command:" + cmdLine + "exeute timeout!");
		}
		return new Pair<Boolean, Pair<String, String>>(!executor.isFailure(exitValue), new Pair<>(new String(
		      out.toByteArray()), new String(err.toByteArray())));
	}

	/**
	 * 
	 * @param destDir
	 * @param jarFile
	 * @throws IOException
	 */
	public void jar(final Path destDir, Path jarFile) throws IOException {
		logger.debug("jar destDir {}, jarFile {}", destDir.getFileName(), jarFile.getFileName());
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VENDOR, "com.ctrip");
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_TITLE, "Avro Schema");
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "");
		manifest.getMainAttributes().put(Attributes.Name.CLASS_PATH, "com.ctrip");
		manifest.getMainAttributes().put(Attributes.Name.SPECIFICATION_VENDOR, "com.ctrip");
		manifest.getMainAttributes().put(Attributes.Name.SPECIFICATION_TITLE, "Avro Schema");
		manifest.getMainAttributes().put(Attributes.Name.SPECIFICATION_VERSION, "");
		final JarOutputStream target = new JarOutputStream(new FileOutputStream(jarFile.toFile()), manifest);

		Files.walkFileTree(destDir, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attrs) throws IOException {
				File file = path.toFile();
				Path pathRelative = destDir.relativize(path);
				String name = pathRelative.toString().replace("\\", "/");

				if (!name.isEmpty()) {
					if (!name.endsWith("/"))
						name += "/";
					JarEntry entry = new JarEntry(name);
					entry.setTime(file.lastModified());
					target.putNextEntry(entry);
					target.closeEntry();
				}
				return super.preVisitDirectory(path, attrs);
			}

			@Override
			public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
				File file = path.toFile();
				Path pathRelative = destDir.relativize(path);
				String name = pathRelative.toString().replace("\\", "/");

				JarEntry entry = new JarEntry(name);
				entry.setTime(file.lastModified());
				target.putNextEntry(entry);
				byte[] readAllBytes = Files.readAllBytes(path);
				target.write(readAllBytes);
				target.closeEntry();
				return super.visitFile(path, attrs);
			}

		});
		target.close();
	}

}
