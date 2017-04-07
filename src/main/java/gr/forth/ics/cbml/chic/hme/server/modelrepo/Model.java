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

import lombok.Data;
import lombok.experimental.Wither;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


@Data
public class Model {
    @Wither
    private final RepositoryId id;
    private final String name;
    private final String description;
    private final UUID uuid;
    private final boolean isStronglyCoupled;
    private final boolean frozen;
    @Wither
    private final List<ModelParameter> inputs;
    @Wither
    private final List<ModelParameter> outputs;
/*
    public Model(RepositoryId modelId, String name, String description, UUID uuid, boolean isStronglyCoupled, boolean isFrozen)
    {
        this.id = modelId;
        this.name = name;
        this.description = description;
        this.uuid = uuid;
        this.isStronglyCoupled = isStronglyCoupled;
        this.frozen = isFrozen;
        this.inputs = Collections.emptyList();
        this.outputs = Collections.emptyList();

    }*/
    public JSONObject toJSON() {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", this.id.toJSON());
        jsonObject.put("title", this.name);
        jsonObject.put("uuid", this.uuid.toString());
        jsonObject.put("description", this.description);
        jsonObject.put("comment", "");
        jsonObject.put("frozen", this.frozen);
        jsonObject.put("strongly_coupled", this.isStronglyCoupled);

        jsonObject.put("inPorts", listParamsToJson(this.inputs) );
        jsonObject.put("outPorts", listParamsToJson(this.outputs));

        jsonObject.put("perspectives", this.perspectivesJson());
        return jsonObject;
    }

    private JSONObject perspectivesJson() {
        // TODO fill in the proper perspective values
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("Perspective I", "");
        jsonObject.put("Perspective II", "");
        jsonObject.put("Perspective III", "");
        jsonObject.put("Perspective IV", "");
        jsonObject.put("Perspective V", "");
        jsonObject.put("Perspective VI", "");
        jsonObject.put("Perspective VII", "");
        jsonObject.put("Perspective VIII", "");
        jsonObject.put("Perspective IX", "");
        jsonObject.put("Perspective X", "");
        jsonObject.put("Perspective XI", "");
        jsonObject.put("Perspective XII", "");
        jsonObject.put("Perspective XIII", "");

        return jsonObject;
    }

    private static JSONArray listParamsToJson(List<ModelParameter> params)
    {
        final List<JSONObject> list = params == null ? Collections.emptyList() : params.stream().map(ModelParameter::toJson).collect(Collectors.toList());

        final JSONArray jsonArray = new JSONArray();
        jsonArray.addAll(list);
        return jsonArray;

    }
}
