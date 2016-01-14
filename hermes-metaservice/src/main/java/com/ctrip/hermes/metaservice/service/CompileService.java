package com.ctrip.hermes.metaservice.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
import org.codehaus.jackson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

@Named
public class CompileService {

	private static final Logger logger = LoggerFactory.getLogger(CompileService.class);

	private JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

	private static final String RELEASE_REPO = "http://maven.dev.sh.ctripcorp.com:8081/nexus/content/repositories/fxrelease";

	private static final String SNAPSHORT_REPO = "http://maven.dev.sh.ctripcorp.com:8081/nexus/content/repositories/fxsnapshot";

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
	      throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("mvn deploy:deploy-file ").append("-DgroupId=").append(groupId).append(" ");
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
		executeConsoleCommand(command);
	}

	private void executeConsoleCommand(String command) throws IOException {
		String osName = System.getProperty("os.name").toLowerCase(Locale.US);
		if (osName.contains("windows")) {
			command = "cmd /c " + command;
		}
		boolean done = false;
		boolean hasErr = false;
		ProcessBuilder pb = new ProcessBuilder(command.split(" "));
		Process process = null;
		StringBuilder errorMsg = new StringBuilder();
		try {
			process = pb.start();
			String s;
			BufferedReader errors = new BufferedReader(new InputStreamReader(process.getErrorStream()));
			while ((s = errors.readLine()) != null) {
				logger.error(s);
				errorMsg.append(s).append("\n");
				hasErr = true;
			}
			done = true;
		} finally {
			if (!done) {
				logger.error(pb.environment().toString());
			}
			if (process != null) {
				process.destroy();
			}
			if (hasErr) {
				throw new RuntimeException(errorMsg.toString());
			}
			if(process!=null&&process.exitValue()!=0){
				throw new RuntimeException("Deploy maven failed! Maybe this dependency already exist on maven.");
			}
		}
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
		manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VENDOR_ID, "com.ctrip");
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
