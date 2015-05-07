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

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.distributedloadsimulator.sls.SLSRunner.LOG;
import org.apache.hadoop.distributedloadsimulator.sls.conf.SLSConfiguration;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.TaskRunner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author sri
 */
// this class will be called by each individual application master process
public class AppMasterProcess {

  private final Configuration conf;
  private final Map<String, Class> amClassMap;
  protected static Configuration applicationMasterConf = new Configuration();
  private int appSimulatorId;
  private ApplicationMasterProtocol appMasterProtocol;
  private static final TaskRunner runner = new TaskRunner();
  private long maxRuntime;
  private int numNMs, numRacks, numAMs, numTasks;
  private ApplicationId applicationId;
  private ApplicationAttemptId appAttemptId;

  public AppMasterProcess() throws ClassNotFoundException {
    conf = new Configuration(false);
    conf.addResource("sls-runner.xml");
    int poolSize = conf.getInt(SLSConfiguration.RUNNER_POOL_SIZE,
            SLSConfiguration.RUNNER_POOL_SIZE_DEFAULT);
    AppMasterProcess.runner.setQueueSize(poolSize);
    amClassMap = new HashMap<String, Class>();
    for (Map.Entry e : conf) {
      String key = e.getKey().toString();
      if (key.startsWith(SLSConfiguration.AM_TYPE)) {
        String amType = key.substring(SLSConfiguration.AM_TYPE.length());
        amClassMap.put(amType, Class.forName(conf.get(key)));
      }
    }
  }
  @SuppressWarnings("unchecked")
  private void startAMFromSLSTraces(Resource containerResource,
          int heartbeatInterval, String inputTrace) throws IOException {
    // parse from sls traces
    int localAppSimulatorOffSet = 0;
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Reader input = new FileReader(inputTrace);
    try {
      Iterator<Map> i = mapper.readValues(jsonF.createJsonParser(input),
              Map.class);
      while (i.hasNext()) {
        Map jsonJob = i.next();
        if (localAppSimulatorOffSet == appSimulatorId) {
          // load job information
          long jobStartTime = Long.parseLong(
                  jsonJob.get("job.start.ms").toString());
          long jobFinishTime = Long.parseLong(
                  jsonJob.get("job.end.ms").toString());

          String user = (String) jsonJob.get("job.user");
          if (user == null) {
            user = "default";
          }
          String queue = jsonJob.get("job.queue.name").toString();

          String oldAppId = jsonJob.get("job.id").toString();
          // boolean isTracked = trackedApps.contains(oldAppId);
          //int queueSize = queueAppNumMap.containsKey(queue)
          //? queueAppNumMap.get(queue) : 0;
          // queueSize++;
          // queueAppNumMap.put(queue, queueSize);
          // tasks
          List tasks = (List) jsonJob.get("job.tasks");
          if (tasks == null || tasks.size() == 0) {
            continue;
          }
          List<ContainerSimulator> containerList
                  = new ArrayList<ContainerSimulator>();
          for (Object o : tasks) {
            Map jsonTask = (Map) o;
            String hostname = jsonTask.get("container.host").toString();
            long taskStart = Long.parseLong(
                    jsonTask.get("container.start.ms").toString());
            long taskFinish = Long.parseLong(
                    jsonTask.get("container.end.ms").toString());
            long lifeTime = taskFinish - taskStart;
            int priority = Integer.parseInt(
                    jsonTask.get("container.priority").toString());
            String type = jsonTask.get("container.type").toString();
            containerList.add(new ContainerSimulator(containerResource,
                    lifeTime, hostname, priority, type));
          }

          // create a new AM
          //this for testing
          boolean isTracked = false;
          //we are pssing rm object as null, we should get rid of rm object
          String amType = jsonJob.get("am.type").toString();
          AMSimulator amSim = (AMSimulator) ReflectionUtils.newInstance(
                  amClassMap.get(amType), new Configuration());
          if (amSim != null) {
            amSim.init(appSimulatorId, heartbeatInterval, containerList, null,
                    null, jobStartTime, jobFinishTime, user, queue,
                    isTracked, oldAppId, appMasterProtocol, applicationId);
            runner.schedule(amSim);
            maxRuntime = Math.max(maxRuntime, jobFinishTime);
            numTasks += containerList.size();
            //amMap.put(oldAppId, amSim);
          }
        }
        ++localAppSimulatorOffSet;
      }
    } finally {
      input.close();
    }
  }

  public void initAMProtocol(String rmAddress) throws IOException {

    applicationMasterConf.setStrings(YarnConfiguration.RM_ADDRESS, rmAddress);
    appMasterProtocol = ClientRMProxy.createRMProxy(applicationMasterConf,
            ApplicationMasterProtocol.class);

    LOG.info("HOP :: Application master protocol is created to submit application : " + appMasterProtocol);

  }

  public void startAM(String inputTraces, int appSimulatorIdOffSet, long clusterTimeStamp, int suffixId, int attemptId) throws IOException {

    this.appSimulatorId = appSimulatorIdOffSet;
    this.applicationId = ApplicationId.newInstance(clusterTimeStamp, suffixId);
    this.appAttemptId = ApplicationAttemptId.newInstance(applicationId, attemptId);

    LOG.info("HOP :: Application id is constructed : " + applicationId.toString() + "| AppAttempID : " + appAttemptId);
    int heartbeatInterval = conf.getInt(
            SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS,
            SLSConfiguration.AM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    int containerMemoryMB = conf.getInt(SLSConfiguration.CONTAINER_MEMORY_MB,
            SLSConfiguration.CONTAINER_MEMORY_MB_DEFAULT);
    int containerVCores = conf.getInt(SLSConfiguration.CONTAINER_VCORES,
            SLSConfiguration.CONTAINER_VCORES_DEFAULT);
    Resource containerResource
            = BuilderUtils.newResource(containerMemoryMB, containerVCores);

    // application workload
    startAMFromSLSTraces(containerResource, heartbeatInterval, inputTraces);

  }

  public static void main(String[] args) throws ClassNotFoundException, IOException {

    // arg[0] - inputTrace
    // arg[1] - applicationSimulatorId
    // arg[2] - resource manager address
    // arg[3] - cluster time stamp
    // arg[4] - application suffix id
    // arg[5] - application attempt id
    AppMasterProcess appMasterProcess = new AppMasterProcess();
    int appSimulatorId = Integer.parseInt(args[1]);

    appMasterProcess.initAMProtocol(args[2]);
    appMasterProcess.startAM(args[0], appSimulatorId, Long.parseLong(args[3]), Integer.parseInt(args[4]), Integer.parseInt(args[5]));

    runner.start();
  }

}
