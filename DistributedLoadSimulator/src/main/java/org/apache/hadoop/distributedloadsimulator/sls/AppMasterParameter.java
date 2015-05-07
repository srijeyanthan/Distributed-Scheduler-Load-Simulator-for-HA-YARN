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
package org.apache.hadoop.distributedloadsimulator.sls;

/**
 *
 * @author sri
 */
public class AppMasterParameter {
  
  private final  String queueName;
  private final  String inputLoadTraces;
  private final   int appSimOffSet;
  private final  String rmAddress;

  public AppMasterParameter(String queueName, String inputLoadTraces, int appSimOffSet, String rmAddress) {
    this.queueName = queueName;
    this.inputLoadTraces = inputLoadTraces;
    this.appSimOffSet = appSimOffSet;
    this.rmAddress = rmAddress;
  }

  public String getQueueName() {
    return queueName;
  }

  public String getInputLoadTraces() {
    return inputLoadTraces;
  }

  public int getAppSimOffSet() {
    return appSimOffSet;
  }

  public String getRmAddress() {
    return rmAddress;
  }
  
  
  
}
