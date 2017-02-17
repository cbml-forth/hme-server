/*
 * Copyright 2016-2017 FORTH-ICS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gr.forth.ics.cbml.chic.hme.server.modelrepo;

import lombok.Value;
import lombok.experimental.Wither;
import lombok.val;
import net.minidev.json.JSONObject;

import java.util.UUID;

@Value
public class Output {
    int id;
    String name;
    UUID uuid;
    String description;
    boolean mandatory;
    String defaultValue;
    boolean dynamic;
    String dataType;
    String unit;
    String range;
    @Wither String value;
    

    public static Output fromJson(JSONObject jsonObject) {

        val id = jsonObject.getAsNumber("id").intValue();
        val dataType = jsonObject.getAsString("data_type");
        val defValue = jsonObject.getAsString("default_value");
        val description = jsonObject.getAsString("description");
        val dynamic = jsonObject.getAsNumber("is_static").intValue() == 0;
        val mandatory = jsonObject.getAsNumber("is_mandatory").intValue() != 0;
        val name = jsonObject.getAsString("name");
        val unit = jsonObject.getAsString("unit");
        val range = jsonObject.getAsString("data_range");
        val uuid = UUID.fromString(jsonObject.getAsString("uuid"));
        val value = "";
        Output p = new Output(id, name, uuid, description, mandatory, defValue, dynamic, dataType, unit, range, value);
        return p;
    }

    public int getId() {
        return this.id;
    }
}
