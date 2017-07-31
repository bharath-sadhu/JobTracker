package org.com.metrics;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class AppMasterDetails {

	public static void main(String[] args) {
		Client client = ClientBuilder.newClient();
		AppMasterDetails ob = new AppMasterDetails();
		try {
			JSONArray apps = ob.getRunningApps(client);
			ob.getApplicationMaster(apps);
		} catch (JSONException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	JSONArray getRunningApps(Client client) throws JSONException {
		// Resource manager url
		WebTarget target = client.target("http://localhost:8088/ws/v1/cluster/apps");
		JSONObject jsonObject = new JSONObject(target.request(MediaType.APPLICATION_JSON).get(String.class));
		JSONArray jsonArray = jsonObject.getJSONArray("apps");
		return jsonArray;
	}

	void getApplicationMaster(JSONArray array) throws JSONException {
		for (int i = 0; i < array.length(); i++) {
			JSONObject jsonObject = array.getJSONObject(i);
			String url = jsonObject.get("trackingUrl").toString();
			printMasterUrlAndPort(url);
		}
	}

	void printMasterUrlAndPort(String url) {
		// tracking url -
		// http://localhost:50070/proxy/application_1326815542473_0001/jobhistory/job/job_1326815542473_1_1
		Pattern pattern = Pattern.compile("\\w+:\\/\\/([\\w+,\\.]*):(\\d+)[\\w+,\\/]*");
		Matcher matcher = pattern.matcher(url);
		while (matcher.find()) {
			System.out.println(matcher.group(1));
			System.out.println(matcher.group(2));
		}
	}
}
