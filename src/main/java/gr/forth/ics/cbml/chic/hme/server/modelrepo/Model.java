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

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.util.List;

/**
 *
 * @author vkrits
 */
public class Model {

    private final String id;
    private final String name;
    private final String description;
    private final String uuid;
    private List<Input> inputs;
    private List<Output> outputs;
    private boolean frozen = false;

    public Model()
    { this("", "", "", "");}

    public Model(final String id, final String name, final String description, final String uuid)
    {
        this.id = id;
        this.name = name;
        this.description = description;
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    /*
    public void setDescription(String description) {
        this.description = description;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setId(String id) {
        this.id = id;
    }
    */

    public List<Input> getInputs() {
        return inputs;
    }

    public void setInputs(List<Input> inputs) {
        this.inputs = inputs;
    }

    public List<Output> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<Output> outputs) {
        this.outputs = outputs;
    }

    public JSONObject toJSON () {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", Integer.parseInt(this.getId())); // XXX
        jsonObject.put("title", this.getName());
        jsonObject.put("uuid", this.getUuid());
        jsonObject.put("description", this.getDescription());
        jsonObject.put("freezed", this.frozen);

        final JSONArray inPorts = new JSONArray();
        this.inputs.forEach(input -> {
            final JSONObject param = new JSONObject();
            param.put("name", input.getName());
            param.put("is_dynamic", input.isDynamic());
            param.put("data_type", input.getDataType());
            param.put("unit", input.getUnit());
            param.put("description", input.getDescription());
            inPorts.add(param);
        });
        jsonObject.put("inPorts", inPorts);
        final JSONArray outPorts = new JSONArray();
        this.outputs.forEach(input -> {
            final JSONObject param = new JSONObject();
            param.put("name", input.getName());
            param.put("is_dynamic", input.isDynamic());
            param.put("data_type", input.getDataType());
            param.put("unit", input.getUnit());
            param.put("description", input.getDescription());
            outPorts.add(param);
        });
        jsonObject.put("outPorts", outPorts);

        return jsonObject;
    }
}
