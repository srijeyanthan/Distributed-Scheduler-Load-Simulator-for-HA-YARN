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
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.log4j.Logger;

import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ContainerSimulator;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.ResourceSchedulerWrapper;
import org.apache.hadoop.distributedloadsimulator.sls.SLSRunner;
import org.apache.hadoop.distributedloadsimulator.sls.scheduler.TaskRunner;
import org.apache.hadoop.distributedloadsimulator.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.util.Records;

public abstract class AMSimulator extends TaskRunner.Task {

  // resource manager
  protected ResourceManager rm;
  // main
  protected SLSRunner se;
  // application
  protected ApplicationId appId;
  protected ApplicationAttemptId appAttemptId;
  protected String oldAppId;    // jobId from the jobhistory file
  // record factory
  protected final static RecordFactory recordFactory
          = RecordFactoryProvider.getRecordFactory(null);
  // response queue
  protected final BlockingQueue<AllocateResponse> responseQueue;
  protected static int RESPONSE_ID = 1;
  // user name
  protected String user;
  // queue name
  protected String queue;
  // am type
  protected String amtype;
  // job start/end time
  protected long traceStartTimeMS;
  protected long traceFinishTimeMS;
  protected long simulateStartTimeMS;
  protected long simulateFinishTimeMS;
  // whether tracked in Metrics
  protected boolean isTracked;
  // progress
  protected int totalContainers;
  protected int finishedContainers;

  private ApplicationMasterProtocol appMasterProtocol;

  private InitializeAppMaster appMaster;
  protected final Logger LOG = Logger.getLogger(AMSimulator.class);
  private int amId;

  public AMSimulator() {
    this.responseQueue = new LinkedBlockingQueue<AllocateResponse>();
  }

  public void init(int id, int heartbeatInterval,
          List<ContainerSimulator> containerList, ResourceManager rm, SLSRunner se,
          long traceStartTime, long traceFinishTime, String user, String queue,
          boolean isTracked, String oldAppId, ApplicationMasterProtocol applicatonMasterProtocol, ApplicationId applicationId) {
    super.init(traceStartTime, traceStartTime + 1000000L * heartbeatInterval,
            heartbeatInterval);
    this.user = user;
    this.amId = id;
    this.appMasterProtocol = applicatonMasterProtocol;
    this.rm = rm;
    this.se = se;
    this.user = user;
    this.queue = queue;
    this.oldAppId = oldAppId;
    this.isTracked = isTracked;
    this.traceStartTimeMS = traceStartTime;
    this.traceFinishTimeMS = traceFinishTime;
    this.appId = applicationId;
  }

  /**
   * register with RM
   *
   * @throws org.apache.hadoop.yarn.exceptions.YarnException
   * @throws java.io.IOException
   * @throws java.lang.InterruptedException
   */
  @Override
  public void firstStep()
          throws YarnException, IOException, InterruptedException {
    simulateStartTimeMS = System.currentTimeMillis()
            - SLSRunner.getRunner().getStartTimeMS();

    // submit application, waiting until ACCEPTED
    submitApp();

    // register application master
    registerAM();

    // track app metrics
    trackApp();
  }

  @Override
  public void middleStep()
          throws InterruptedException, YarnException, IOException {
    // process responses in the queue
    processResponseQueue();

    // send out request
    sendContainerRequest();

    // check whether finish
    checkStop();
  }

  @Override
  public void lastStep() throws YarnException {
    LOG.info(MessageFormat.format("Application {0} is shutting down.", appId));
    // unregister tracking
    if (isTracked) {
      untrackApp();
    }
    // unregister application master
    final FinishApplicationMasterRequest finishAMRequest = recordFactory
            .newRecordInstance(FinishApplicationMasterRequest.class);
    finishAMRequest.setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
    try {
      appMasterProtocol.finishApplicationMaster(finishAMRequest);
    } catch (IOException ex) {
      LOG.error("Exception in calling finish applicatoin master ", ex);
    }
    if (rm != null) {
      simulateFinishTimeMS = System.currentTimeMillis()
              - SLSRunner.getRunner().getStartTimeMS();
      // record job running information
      ((ResourceSchedulerWrapper) rm.getResourceScheduler())
              .addAMRuntime(appId,
                      traceStartTimeMS, traceFinishTimeMS,
                      simulateStartTimeMS, simulateFinishTimeMS);
    }
  }

  protected ResourceRequest createResourceRequest(
          Resource resource, String host, int priority, int numContainers) {
    ResourceRequest request = recordFactory
            .newRecordInstance(ResourceRequest.class);
    request.setCapability(resource);
    request.setResourceName(host);
    request.setNumContainers(numContainers);
    Priority prio = recordFactory.newRecordInstance(Priority.class);
    prio.setPriority(priority);
    request.setPriority(prio);
    return request;
  }

  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask,
          List<ContainerId> toRelease) {
    AllocateRequest allocateRequest
            = recordFactory.newRecordInstance(AllocateRequest.class);
    allocateRequest.setResponseId(RESPONSE_ID++);
    allocateRequest.setAskList(ask);
    allocateRequest.setReleaseList(toRelease);
    return allocateRequest;
  }

  protected AllocateRequest createAllocateRequest(List<ResourceRequest> ask) {
    return createAllocateRequest(ask, new ArrayList<ContainerId>());
  }

  protected abstract void processResponseQueue()
          throws InterruptedException, YarnException, IOException;

  protected abstract void sendContainerRequest()
          throws YarnException, IOException, InterruptedException;

  protected abstract void checkStop();

  public void registerAM() throws YarnException, IOException {
    final RegisterApplicationMasterRequest amRegisterRequest
            = Records.newRecord(RegisterApplicationMasterRequest.class);
    amRegisterRequest.setHost("localhost");
    amRegisterRequest.setRpcPort(1000);
    amRegisterRequest.setTrackingUrl("localhost:1000");
    RegisterApplicationMasterResponse response = appMasterProtocol.registerApplicationMaster(amRegisterRequest);
    LOG.info("HOP :: Registered application master success <Maximum Resource Capability> : " + response.getMaximumResourceCapability());
  }

  private void submitApp()
          throws YarnException, InterruptedException, IOException {
  }

  public void trackApp() {
    if (isTracked) {
      // if we are running load simulator alone, rm is null
      if (rm != null) {
        ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                .addTrackedApp(appAttemptId, oldAppId);
      }
    }
  }

  public void untrackApp() {
    if (isTracked) {
      // if we are running load simulator alone, rm is null
      if (rm != null) {
        ((ResourceSchedulerWrapper) rm.getResourceScheduler())
                .removeTrackedApp(appAttemptId, oldAppId);
      }
    }
  }

  protected List<ResourceRequest> packageRequests(
          List<ContainerSimulator> csList, int priority) {
    // create requests
    Map<String, ResourceRequest> rackLocalRequestMap = new HashMap<String, ResourceRequest>();
    Map<String, ResourceRequest> nodeLocalRequestMap = new HashMap<String, ResourceRequest>();
    ResourceRequest anyRequest = null;
    for (ContainerSimulator cs : csList) {
      String rackHostNames[] = SLSUtils.getRackHostName(cs.getHostname());
      // check rack local
      String rackname = rackHostNames[0];
      if (rackLocalRequestMap.containsKey(rackname)) {
        rackLocalRequestMap.get(rackname).setNumContainers(
                rackLocalRequestMap.get(rackname).getNumContainers() + 1);
      } else {
        ResourceRequest request = createResourceRequest(
                cs.getResource(), rackname, priority, 1);
        rackLocalRequestMap.put(rackname, request);
      }
      // check node local
      String hostname = rackHostNames[1];
      if (nodeLocalRequestMap.containsKey(hostname)) {
        nodeLocalRequestMap.get(hostname).setNumContainers(
                nodeLocalRequestMap.get(hostname).getNumContainers() + 1);
      } else {
        ResourceRequest request = createResourceRequest(
                cs.getResource(), hostname, priority, 1);
        nodeLocalRequestMap.put(hostname, request);
      }
      // any
      if (anyRequest == null) {
        anyRequest = createResourceRequest(
                cs.getResource(), ResourceRequest.ANY, priority, 1);
      } else {
        anyRequest.setNumContainers(anyRequest.getNumContainers() + 1);
      }
    }
    List<ResourceRequest> ask = new ArrayList<ResourceRequest>();
    ask.addAll(nodeLocalRequestMap.values());
    ask.addAll(rackLocalRequestMap.values());
    if (anyRequest != null) {
      ask.add(anyRequest);
    }
    return ask;
  }

  public String getQueue() {
    return queue;
  }

  public String getAMType() {
    return amtype;
  }

  public long getDuration() {
    return simulateFinishTimeMS - simulateStartTimeMS;
  }

  public int getNumTasks() {
    return totalContainers;
  }
}
