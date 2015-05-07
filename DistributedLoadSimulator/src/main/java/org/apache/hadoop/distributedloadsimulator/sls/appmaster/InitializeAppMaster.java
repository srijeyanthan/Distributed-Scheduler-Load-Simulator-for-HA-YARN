/*
 * Copyright 2015 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.distributedloadsimulator.sls.appmaster;

/**
 *
 * @author sri
 */
import java.io.File;
import java.io.IOException;
import static java.lang.Thread.sleep;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class InitializeAppMaster implements Runnable {

  private static final Log LOG = LogFactory
          .getLog(InitializeAppMaster.class);

  protected static Configuration conf = new YarnConfiguration();
  private UnmanagedAMLauncher launcher;
  private String queueName;
  private String inputLoadTraces;
  private int slsLoad;
  private int appSimOffSet;
  private String rmAddress;

  public InitializeAppMaster(String queueName, String inputLoadTraces, int appSimOffSet, String rmAddress) {
    this.queueName = queueName;
    this.inputLoadTraces = inputLoadTraces;
    this.appSimOffSet = appSimOffSet;
    this.rmAddress = rmAddress;
  }

  private String getRunTimeClassPath() {
    LOG.info("Trying to generate classpath for app master from current thread's classpath");
    String envClassPath = "";
    String cp = System.getProperty("java.class.path");
    if (cp != null) {
      envClassPath += cp.trim() + File.pathSeparator;
    }
    ClassLoader thisClassLoader = Thread.currentThread()
            .getContextClassLoader();
    URL url = thisClassLoader.getResource("yarn-site.xml");
    envClassPath += new File(url.getFile()).getParent();
    return envClassPath;
  }

  public void LaunchApplication() throws Exception {
    String classpath = "/home/sri/Thesis/ha-yarn";
//getRunTimeClassPath();
    String javaHome = System.getenv("JAVA_HOME");
    if (javaHome == null) {
      LOG.fatal("JAVA_HOME not defined. Test not running.");
      return;
    }
    String[] args = {
      "--classpath",
      classpath,
      "--queue",
      queueName,
      "--cmd",
      javaHome
      + "/bin/java -Xmx512m "
      + AppMasterProcess.class.getCanonicalName() + " "
      + inputLoadTraces + " "
      + Integer.toString(appSimOffSet) + " "
      + rmAddress};

    LOG.info("Initializing Launcher : setting resource manger address : " + rmAddress);
    conf.setStrings(YarnConfiguration.RM_ADDRESS,rmAddress);
    launcher
            = new UnmanagedAMLauncher(conf) {
              @Override
              public void launchAM(ApplicationAttemptId attemptId)
              throws IOException, YarnException {
                YarnApplicationAttemptState attemptState
                = rmClient.getApplicationAttemptReport(attemptId)
                .getYarnApplicationAttemptState();
                super.launchAM(attemptId);
              }
            };
    boolean initSuccess = launcher.init(args);
    LOG.info("HOP :: Launcher initialized , result : " + initSuccess);
    boolean result = launcher.run();
    LOG.info("Launcher run completed. Result=" + result);

  }

  @Override
  public void run() {
    try {
      LaunchApplication();
    } catch (Exception ex) {
      Logger.getLogger(InitializeAppMaster.class.getName()).log(Level.SEVERE, null, ex);
    }
    while (true) {
      try {
        sleep(100);
      } catch (InterruptedException ex) {
        Logger.getLogger(InitializeAppMaster.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }
}
