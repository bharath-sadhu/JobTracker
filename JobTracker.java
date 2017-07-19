package com.hadoop.rest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.json.JSONArray;
import org.json.JSONObject;

//Replace proxy http address:port with with resource manager IP and port corresponding to Resources
public class JobTracker {
	public static void main(String args[]) throws InterruptedException {
		JobTracker ob = new JobTracker();
		Client client = ClientBuilder.newClient();
		// Application id of first application.
		String applicationId = ob.getApplications(client).get(0);
		// Application id of first job.
		String jobId = ob.getJobs(client, applicationId).get(0);

		Set<String> finishedTasks = new HashSet<>();
		while (true) {
			Set<String> currentFinishedTasks = ob.getFinishedTasks(client, applicationId, jobId, finishedTasks);
			for (String taskId : currentFinishedTasks) {
				ob.getTaskCOunters(client, applicationId, jobId, taskId);
				ob.getNodeOnwhichTaskFinished(client, applicationId, jobId, taskId);
			}
			finishedTasks.addAll(currentFinishedTasks);
			// poll tasks for every 10 seconds
			Thread.sleep(10000);
		}
	}

	/**
	 * Gets all application ID's from resource manager
	 * 
	 * @param client
	 * @return Application ID's
	 */
	public List<String> getApplications(Client client) {
		WebTarget target;
		List<String> applicationIds = new ArrayList<String>();
		// Resource manager URL
		target = client.target("http://<rm http address:port>/ws/v1/cluster/apps");

		JSONObject obj = new JSONObject(target.request(MediaType.APPLICATION_JSON).get(String.class));
		JSONArray array = obj.getJSONObject("apps").getJSONArray("app");
		for (int i = 0; i < array.length(); i++) {
			applicationIds.add((String) array.getJSONObject(i).get("id"));
		}
		return applicationIds;
	}

	/*
	 * Returns all jobs corresponding to application
	 */
	public List<String> getJobs(Client client, String applicationId) {
		WebTarget target;
		List<String> jobIds = new ArrayList<String>();
		target = client.target("http://<proxy http address:port>/proxy/" + applicationId + "/ws/v1/mapreduce/jobs");
		JSONObject obj = new JSONObject(target.request(MediaType.APPLICATION_JSON).get(String.class));
		JSONArray array = obj.getJSONObject("jobs").getJSONArray("job");
		for (int i = 0; i < array.length(); i++) {
			jobIds.add((String) array.getJSONObject(i).get("id"));
		}
		return jobIds;
	}

	/*
	 * Returns finished tasks of a job
	 */
	public Set<String> getFinishedTasks(Client client, String applicationId, String jobId, Set<String> finishedTasks) {
		Set<String> taskIds = new HashSet<>();
		WebTarget target;
		target = client.target("http://<proxy http address:port>/proxy/" + applicationId + "/ws/v1/mapreduce/jobs/"
				+ jobId + "/tasks");
		JSONObject obj = new JSONObject(target.request(MediaType.APPLICATION_JSON).get(String.class));
		JSONArray array = obj.getJSONObject("tasks").getJSONArray("task");
		for (int i = 0; i < array.length(); i++) {
			JSONObject currentObj = array.getJSONObject(i);
			if (currentObj.get("state") == "SUCCEEDED" && !finishedTasks.contains(currentObj.get("id"))) {
				taskIds.add((String) array.getJSONObject(i).get("id"));
			}
		}
		return taskIds;
	}

	/*
	 * Prints task counter json object
	 */
	public void getTaskCOunters(Client client, String applicationId, String jobId, String taskId) {
		WebTarget target;
		target = client.target("http://<proxy http address:port>/proxy/" + applicationId + "/ws/v1/mapreduce/jobs/"
				+ jobId + "/tasks/" + taskId + "/counters");
		// prints all counters
		System.out.println(target.request(MediaType.APPLICATION_JSON).get(String.class));
	}

	/*
	 * Prints Node details on which task is finished
	 */
	public void getNodeOnwhichTaskFinished(Client client, String applicationId, String jobId, String taskId) {
		WebTarget target;
		target = client.target("http://<proxy http address:port>/proxy/" + applicationId + "/ws/v1/mapreduce/jobs/"
				+ jobId + "/tasks/" + taskId + "attempts");
		JSONObject obj = new JSONObject(target.request(MediaType.APPLICATION_JSON).get(String.class));
		JSONArray array = obj.getJSONObject("taskAttempts").getJSONArray("taskAttempt");
		JSONObject taskObj = array.getJSONObject(array.length() - 1);
		System.out.println("Node on which task ran is, rack: " + taskObj.get("rack") + " and ip :"
				+ taskObj.get("nodeHttpAddress"));
	}
}
