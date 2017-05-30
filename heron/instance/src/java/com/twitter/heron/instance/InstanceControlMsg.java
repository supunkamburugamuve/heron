// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.instance;

import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;

public final class InstanceControlMsg {
  public enum ControlType {
    STREAM_CONNECT,
    INSTANCE_CONNECT_IN,
    INSTANCE_CONNECT_OUT
  }

  private PhysicalPlanHelper newPhysicalPlanHelper;
  private ControlType controlType = ControlType.STREAM_CONNECT;
  private int taskId;

  private InstanceControlMsg(Builder builder) {
    this.newPhysicalPlanHelper = builder.newPhysicalPlanHelper;
    this.controlType = builder.controlType;
    this.taskId = builder.taskId;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public PhysicalPlanHelper getNewPhysicalPlanHelper() {
    return newPhysicalPlanHelper;
  }

  public ControlType getControlType() {
    return controlType;
  }

  public int getTaskId() {
    return taskId;
  }

  public boolean isNewPhysicalPlanHelper() {
    return newPhysicalPlanHelper != null;
  }

  public static final class Builder {
    private PhysicalPlanHelper newPhysicalPlanHelper;
    private int taskId;
    private ControlType controlType = ControlType.STREAM_CONNECT;

    private Builder() {
    }

    public Builder setInTaskConnect(int instance) {
      this.taskId = instance;
      this.controlType = ControlType.INSTANCE_CONNECT_IN;
      return this;
    }

    public Builder setOutTaskConnect(int instance) {
      this.taskId = instance;
      this.controlType = ControlType.INSTANCE_CONNECT_OUT;
      return this;
    }

    public Builder setNewPhysicalPlanHelper(PhysicalPlanHelper physicalPlanHelper) {
      this.newPhysicalPlanHelper = physicalPlanHelper;
      this.controlType = ControlType.STREAM_CONNECT;
      return this;
    }

    public InstanceControlMsg build() {
      return new InstanceControlMsg(this);
    }
  }
}
