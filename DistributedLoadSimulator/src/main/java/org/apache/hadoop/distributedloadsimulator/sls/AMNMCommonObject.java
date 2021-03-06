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

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author sri
 */
public interface AMNMCommonObject extends Remote {
  
 // all the unmanged application master will call this to update node manager's containers 
 // internal data strcutres
  void cleanupContainer(String containerId, String nodeId) throws RemoteException;
  
  void addNewContainer(String containerId, String nodeId, String httpAddress,
          int memory, int vcores, int priority,long lifeTimeMS) throws RemoteException;
  
}
