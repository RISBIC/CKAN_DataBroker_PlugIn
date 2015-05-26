/* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
* <p/>
* http://www.apache.org/licenses/LICENSE-2.0
* <p/>
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.risbic.dkan;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.util.Iterator;

public class DKANConnection
{
   private static final String AUTH_PATH = "/api/action/datastore/user/login";

   private static final String FILE_PATH = "/api/action/datastore/file";

   private static final String NODES_PATH = "/api/action/datastore/node";

   private String url;

   private HttpClient client;

   private String csrfToken;

   private String cookie;

   public DKANConnection(String username, String password, String url) throws Exception
   {
      this.url = url;
      client = new HttpClient();
      authenticate(username, password);
   }

   public boolean authenticate(String username, String password) throws IOException
   {
      String endpoint = url + AUTH_PATH;
      PostMethod post = new PostMethod(endpoint);
      post.setRequestHeader("Accept", "application/json");

      NameValuePair[] data = new NameValuePair[2];
      data[0] = new NameValuePair("username", username);
      data[1] = new NameValuePair("password", password);

      post.setRequestBody(data);

      client.executeMethod(post);
      cookie = post.getResponseHeader("Set-Cookie").getValue();
      this.csrfToken = ((JsonObject) readJSONResponse(post, false)).getString("token");

      return csrfToken != null;
   }

   public boolean authenticated()
   {
      return csrfToken != null;
   }

   public String createFile(File file) throws IOException
   {
      return createFile(Files.readAllBytes(file.toPath()), file.getName());
   }

   public String createFile(byte[] data, String filename) throws IOException
   {
      PostMethod post = createPostMethod(url + FILE_PATH);
      post.addParameter("file", encodeData(data));
      post.addParameter("filename", filename);

      if(client.executeMethod(post) == 200)
      {
         return ((JsonObject) readJSONResponse(post, false)).getString("fid");
      }
      return null;
   }

   public String createDataSet(String title, String description) throws IOException
   {
      PostMethod post = createPostMethod(url + NODES_PATH);
      post.addParameter("type", "dataset");
      post.addParameter("title", title);
      post.addParameter("body[und][0][value]", description);

      if(client.executeMethod(post) == 200)
      {
         return ((JsonObject) readJSONResponse(post, false)).getString("nid");
      }
      return null;
   }

   public String createResourceByDataSetId(String title, String description, String dataSetId, String fileId) throws IOException
   {
      PostMethod post = createPostMethod(url + NODES_PATH);
      post.addParameter("type", "resource");
      post.addParameter("title", title);
      post.addParameter("body[und][0][value]", description);
      post.addParameter("field_dataset_ref[und][]", dataSetId);
      post.addParameter("field_link_remote_file[und][0][fid]", fileId);

      if(client.executeMethod(post) == 200)
      {
         return ((JsonObject) readJSONResponse(post, false)).getString("nid");
      }
      return null;
   }

   public String createResourceByDataSetTitle(String title, String description, String dataSetTitle, String fileId) throws IOException
   {
      String dataSetId = findOrCreateDataSetByTitle(dataSetTitle);
      if (dataSetId != null)
      {
         return createResourceByDataSetId(title, description, dataSetId, fileId);
      }
      return null;
   }

   public String findOrCreateDataSetByTitle(String title) throws IOException
   {
      GetMethod get = createGetMethod(url + NODES_PATH);
      client.executeMethod(get);
      JsonArray dataSets = ((JsonArray) readJSONResponse(get, true));

      String dataSetId = null;

      Iterator<JsonValue> i = dataSets.iterator();
      while(i.hasNext())
      {
         JsonValue value =  i.next();
         if (value instanceof JsonObject)
         {
            JsonObject dataSet = (JsonObject) value;
            if (dataSet.getString("type").equals("dataset") && dataSet.getString("title").equals(title))
            {
               dataSetId = dataSet.getString("nid");
            }
         }
      }

      if (dataSetId == null)
      {
         dataSetId = createDataSet(title, title);
      }
      return dataSetId;
   }

   private static String encodeData(byte[] data) throws UnsupportedEncodingException
   {
      return new String(Base64.encodeBase64(data), "UTF-8");
   }

   private PostMethod createPostMethod(String url)
   {
      PostMethod post = new PostMethod(url);
      post.setRequestHeader("X-CSRF-Token", csrfToken);
      post.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
      post.setRequestHeader("Accept", "application/json");
      post.setRequestHeader("Cookie", cookie);
      return post;
   }

   private GetMethod createGetMethod(String url)
   {
      GetMethod get = new GetMethod(url);
      get.setRequestHeader("X-CSRF-Token", csrfToken);
      get.setRequestHeader("Accept", "application/json");
      get.setRequestHeader("Cookie", cookie);
      return get;
   }

   private JsonStructure readJSONResponse(HttpMethod method, boolean array) throws IOException
   {
      JsonReader jsonReader = Json.createReader(method.getResponseBodyAsStream());
      return array ? jsonReader.readArray() : jsonReader.readObject();
   }
}
