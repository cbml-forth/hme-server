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


import lombok.Builder;
import lombok.Value;
import lombok.experimental.Wither;
import lombok.val;
import net.minidev.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Value
@Builder
public class Input {

    int id;
    UUID uuid;
    String name;
    String description;
    boolean mandatory;
    String defaultValue;
    boolean dynamic;
    String dataType;
    String unit;
    String range;
    @Wither
    String value;

    private List<String> semTypes;

    public Optional<String> getDefaultValue() {
        return Optional.ofNullable(value);
    }

    public Optional<String> getValue() {
        return Optional.ofNullable(value);
    }

    public boolean isPatientSpecific()
    {
        return "file".equalsIgnoreCase(this.dataType) ||
                this.semTypes.stream().anyMatch(s -> s.startsWith("http://chic-vph.eu/#Patient"));
    }

    @Override
    public String toString() {
        return String.format("NAME: %s TYPE: %s UNIT: %s RANGE: %s DEF: %s (%s)",
                this.getName(),
                this.getDataType(),
                this.getUnit(),
                this.getRange(),
                this.getDefaultValue().orElse(""),
                this.getDescription());
    }

    public static Input fromJson(JSONObject jsonObject) {
        val id = jsonObject.getAsNumber("id").intValue();
        val uuid = UUID.fromString(jsonObject.getAsString("uuid"));
        val dataType = jsonObject.getAsString("data_type");
        val defaultValue = jsonObject.getAsString("default_value");
        val description = jsonObject.getAsString("description").replace("\r", "").replace("\n", " ");

        final List<String> semTypes = Arrays.asList(jsonObject.getAsString("semtype").split("\\s+"));

        val dynamic = jsonObject.getAsNumber("is_static").intValue() == 0;

        val mandatory = jsonObject.getAsNumber("is_mandatory").intValue() == 1;
        val name = jsonObject.getAsString("name");
        val range = jsonObject.getAsString("data_range");
        val unit = jsonObject.getAsString("unit");
        final Input input = Input.builder()
                .id(id)
                .uuid(uuid)
                .name(name)
                .description(description)
                .mandatory(mandatory)
                .defaultValue("".equals(defaultValue) ? null : defaultValue)
                .dynamic(dynamic)
                .dataType(dataType)
                .unit(unit)
                .range(range)
                .semTypes(semTypes)
                .build();
        /*
        Input input = new Input(id, uuid, name, description, mandatory,
                "".equals(defaultValue) ? null : defaultValue,
                dynamic, dataType, unit, range, null, semTypes);*/
        return input;
    }
}
