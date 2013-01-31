package com.linkedin.databus3.espresso.client;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import com.linkedin.databus.client.pub.DeregisterResult;
import com.linkedin.databus.client.pub.RegistrationId;

public class DeregisterResultImpl implements DeregisterResult 
{

	private boolean success;
	private String errorMessage;
	private boolean connectionShutdown;
	private Exception exception;
	
	public static DeregisterResult createRegNotFoundResult(RegistrationId id)
	{
		DeregisterResultImpl result = new DeregisterResultImpl();
		result.setSuccess(false);
		result.setErrorMessage("Registration identified by id (" + id + ") is not found in the system !!");
		return result;
	}
	
	public static DeregisterResult createFailedDeregisterResult(RegistrationId id, String errorMessage, Exception ex)
	{
		DeregisterResultImpl result = new DeregisterResultImpl();
		result.setSuccess(false);
		result.setErrorMessage("Deregistration for id (" + id + ") failed. Reason :" + errorMessage);
		result.setException(ex);
		return result;
	}
	
	public static DeregisterResult createSuccessDeregisterResult(boolean isConnectionShutdown)
	{
		DeregisterResultImpl result = new DeregisterResultImpl();
		result.setSuccess(true);
		result.setConnectionShutdown(isConnectionShutdown);
		return result;
	}
	
	public DeregisterResultImpl()
	{
		success = true;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public boolean isConnectionShutdown() {
		return connectionShutdown;
	}

	public Exception getException() {
		return exception;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public void setConnectionShutdown(boolean connectionShutdown) {
		this.connectionShutdown = connectionShutdown;
	}

	public void setException(Exception exception) {
		this.exception = exception;
	}

	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
}
