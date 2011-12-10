package com.notnoop.hadoopsbt

import sbt._
import Path._

import java.io.File
import java.lang.{ProcessBuilder => JProcessBuilder}

import Project.Initialize

object HadoopPlugin extends Plugin {
  object HadoopKeys {
    lazy val hadoopHome = SettingKey[File]("hadoop-home",
      "path to Hadoop installation")
    lazy val hadoopBin = SettingKey[File]("hadoop-bin",
      "path to hadoop executable")
  }

  import HadoopKeys._

  def getHadoopHome = {
    val path =
      System.getProperty("hadoop.home",
        System.getProperty("HADOOP_HOME",
          System.getenv("HADOOP_HOME")))

    if (path == null)
      null
    else
      new java.io.File(path)
  }

  import Keys._
  def runnerInit: Initialize[Task[ScalaRun]] =
    (taskTemporaryDirectory, scalaInstance, baseDirectory, javaOptions,
    outputStrategy, javaHome, trapExit, connectInput, hadoopBin) map {
    (tmp, si, base, options, strategy, javaHomeDir, trap, connectIn,
    hadoop) =>
      new HadoopForkRun(hadoop, ForkOptions(scalaJars = si.jars, javaHome = javaHomeDir, connectInput = connectIn, outputStrategy = strategy,
        runJVMOptions = options, workingDirectory = Some(base)) )
  }


  lazy val hadoopSettings = Seq(
    hadoopHome := getHadoopHome,

    hadoopBin <<= hadoopHome( _ / "bin" / "hadoop" )
  ) ++
    inConfig(Configurations.Compile)(
      Keys.runner in Keys.run <<= runnerInit)
}


class HadoopForkRun(hadoopCmd: File, config: ForkScalaRun) extends ScalaRun
{
	def run(mainClass: String, classpath: Seq[File], options: Seq[String], log: Logger): Option[String] =
	{
		log.info("Running " + mainClass + " " + options.mkString(" "))

		//val scalaOptions = classpathOption(classpath) ::: mainClass :: options.toList
		val strategy = config.outputStrategy getOrElse LoggedOutput(log)
		val process =  scalaFork(config.javaHome, config.runJVMOptions,
        config.scalaJars, classpath, mainClass, options, config.workingDirectory, config.connectInput, strategy)
		def cancel() = {
			log.warn("Run canceled.")
			process.destroy()
			1
		}
		val exitCode = try process.exitValue() catch { case e: InterruptedException => cancel() }
		processExitCode(exitCode, "runner")
	}
	private def classpathOption(classpath: Seq[File]) = "-classpath" :: Path.makeString(classpath) :: Nil
	private def processExitCode(exitCode: Int, label: String) =
	{
		if(exitCode == 0)
			None
		else
			Some("Nonzero exit code returned from " + label + ": " + exitCode)
	}

    def scalaFork(javaHome: Option[File], jvmOptions: Seq[String], scalaJars: Iterable[File],
      classpath: Seq[File], mainClass: String,
      arguments: Seq[String], workingDirectory: Option[File], connectInput: Boolean,
      outputStrategy: OutputStrategy): Process = {
        if(scalaJars.isEmpty) sys.error("Scala jars not specified")

        val scalaClasspathString = "-Xbootclasspath/a:" + scalaJars.map(_.getAbsolutePath).mkString(File.pathSeparator)

      val hadoopOptions = jvmOptions ++ List(scalaClasspathString)
      javaFork(javaHome, classpath, mainClass, hadoopOptions, arguments, workingDirectory, Map.empty, connectInput, outputStrategy)
    }

    def javaFork(javaHome: Option[File], classpath: Seq[File],
      mainClass: String, hadoopOptions: Seq[String],
    arguments: Seq[String], workingDirectory: Option[File],
    env: Map[String, String], connectInput: Boolean,
    outputStrategy: OutputStrategy): Process = {

        val executable = hadoopCmd.getAbsolutePath
        val command = (executable :: mainClass :: arguments.toList).toArray
        println("herehere: " + command.mkString(" "))
        val builder = new JProcessBuilder(command : _*)
        workingDirectory.foreach(wd => builder.directory(wd))
        val environment = builder.environment
        for( (key, value) <- env )
            environment.put(key, value)
        environment.put("HADOOP_OPTS", hadoopOptions.mkString(" "))
        environment.put("HADOOP_CLASSPATH", Path.makeString(classpath))

        outputStrategy match {
            case StdoutOutput => Process(builder).run(connectInput)
            case BufferedOutput(logger) => Process(builder).runBuffered(logger, connectInput)
            case LoggedOutput(logger) => Process(builder).run(logger, connectInput)
            case CustomOutput(output) => (Process(builder) #> output).run(connectInput)
        }
    }


}


