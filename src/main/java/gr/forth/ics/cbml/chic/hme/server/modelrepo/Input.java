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


import java.util.List;
import java.util.Optional;

public class Input {

    private String name;
    private String description;
    private boolean mandatory;
    private Optional<String> defaultValue;
    private boolean dynamic;
    private String dataType;
    private String unit;
    private String range;
    private String value;

    private List<String> semTypes;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isMandatory() {
        return mandatory;
    }

    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }

    public Optional<String> getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = Optional.ofNullable(defaultValue).filter(s -> !"".equals(s));
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    public List<String> getSemTypes() {
        return semTypes;
    }

    public void setSemTypes(List<String> semTypes) {
        this.semTypes = semTypes;
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
                this.getDefaultValue(),
                this.getDescription());
    }
}
