/*
 * Copyright 2019 SMB GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package solutions.Infy.kafka.connect.sendgrid;

import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Version {

  private static final String VERSION_FILE = "/kafka-connect-sendgrid-version.properties";

  private static String version = "unknown";

  static {
    try {
      Properties props = new Properties();
      try (InputStream versionFileStream = Version.class.getResourceAsStream(VERSION_FILE)) {
        props.load(versionFileStream);
        version = props.getProperty("version", version).trim();
      }
    } catch (Exception e) {
      log.warn("Error while loading version:", e);
    }
  }

  public static String getVersion() {
    return version;
  }

}
