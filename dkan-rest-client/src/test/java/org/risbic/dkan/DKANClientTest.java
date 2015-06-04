/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
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

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DKANClientTest
{
   private static final String DKAN_PROPERTIES_FILE = "/dkanapi.properties";

   private static final String DKAN_TEST_UPLOAD_FILE = "/test.csv";

   private static final String DKAN_USERNAME_KEY = "username";

   private static final String DKAN_PASSWORD_KEY = "password";

   private static final String DKAN_URL_KEY = "dkanrooturl";

   private String username;

   private String password;

   private String url;

   @Before
   public void setup()
   {
      try
      {
         Class c = getClass();
         URL url1 = c.getResource(DKAN_PROPERTIES_FILE);
         String file = url1.getFile();

         try (FileReader fr = new FileReader(file))
         {
            Properties properties = new Properties();
            properties.load(fr);

            username = properties.getProperty(DKAN_USERNAME_KEY);
            password = properties.getProperty(DKAN_PASSWORD_KEY);
            url = properties.getProperty(DKAN_URL_KEY);
         }
      }
      catch (Exception e)
      {
         fail("Unable to read properties file.  Please ensure " + DKAN_PROPERTIES_FILE + " exists under test/resources directory");
      }
   }

   @Test
   public void testAuthenticate() throws Exception
   {
      DKANConnection connection = DKANClient.connect(username, password, url);
      assertTrue(connection.authenticated());
   }

   @Test
   public void testFileUpload() throws Exception
   {
      DKANConnection connection = DKANClient.connect(username, password, url);
      File file = new File(getClass().getResource(DKAN_TEST_UPLOAD_FILE).getFile());
      assertNotNull(connection.createFile(file));
   }

   @Test
   public void testCreateDataSet() throws Exception
   {
      DKANConnection connection = DKANClient.connect(username, password, url);
      assertNotNull(connection.createDataSet("TestDataSet_" + UUID.randomUUID().toString(), "Test Description"));
   }

   @Test
   public void testCreateResource() throws Exception
   {
      DKANConnection connection = DKANClient.connect(username, password, url);

      // Create Data Set
      String dataSetId = connection.createDataSet("TestResourceDataSet_" + UUID.randomUUID().toString(), "Desc");

      // Create File
      String fileId = connection.createFile(new File(getClass().getResource(DKAN_TEST_UPLOAD_FILE).getFile()));

      // Create Resource
      String resourceId = connection.createResourceByDataSetId("Res_" + UUID.randomUUID().toString(), "Desc", dataSetId, fileId);
      assertNotNull(resourceId);
   }

   @Test
   public void testFindDataSetByTitle() throws Exception
   {
      DKANConnection connection = DKANClient.connect(username, password, url);

      String title = "Title_" + UUID.randomUUID().toString();
      String expectedDataSetId = connection.createDataSet(title, "Desc");

      String dataSetId = connection.findOrCreateDataSetByTitle(title);

      assertEquals(expectedDataSetId, dataSetId);
   }

   @Test
   public void testCreateResourceByDataSetTitle() throws Exception
   {
      DKANConnection connection = DKANClient.connect(username, password, url);

      // Create Data Set
      String dataSetTitle = "TestResourceDataSet_" + UUID.randomUUID().toString();
      String dataSetId = connection.createDataSet(dataSetTitle, "Desc");

      // Create File
      String fileId = connection.createFile(new File(getClass().getResource("/test.csv").getFile()));

      // Create Resource
      String resourceId = connection.createResourceByDataSetTitle("Res_" + UUID.randomUUID().toString(), "Desc", dataSetTitle, fileId);
      assertNotNull(resourceId);
   }
}
