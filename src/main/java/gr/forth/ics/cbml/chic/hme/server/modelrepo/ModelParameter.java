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
import java.util.stream.Collectors;

@Value
@Builder
public class ModelParameter {

    @Wither
    RepositoryId id;
    UUID uuid;
    String name;
    String description;
    boolean output;
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


    // The following is for sending the JSON representation to the Editor's UI
    // i.e. it's not the reverse of `fromJson`!
    public JSONObject toJson() {
        val m = new JSONObject();
        m.put("id", id.toJSON());
        m.put("uuid", uuid.toString());
        m.put("name", name);
        m.put("data_type", dataType);
        m.put("default_value", defaultValue);
        m.put("description", description);

        m.put("semtype", semTypes); //semTypes.stream().collect(Collectors.joining(" ")));
        m.put("is_output", this.output);
        m.put("is_static", !this.dynamic);
        m.put("is_dynamic", this.dynamic);

        m.put("is_mandatory", this.mandatory);
        m.put("data_range", range);
        m.put("unit", unit);
        return m;

    }

    private static String getStringOrDef(JSONObject jsonObject, String field, String defaultValue)
    {
        final String string = jsonObject.getAsString(field);
        if (string == null)
            return defaultValue;
        return string;
    }
    private static String getStringOrDef(JSONObject jsonObject, String field)
    {
        final String defaultValue = "";
        return getStringOrDef(jsonObject, field, defaultValue);
    }


    private static <T extends Number> T getNumberOrDef(JSONObject jsonObject, String field, T defaultValue)
    {
        return (T) jsonObject.getOrDefault(field, defaultValue);
    }

    // The following reads the JSON sent by the model repository and has a few
    // peculiarities. e.g. boolean fields are encoded as integers, 1 => true,
    // 0 => false, although JSON supports boolean literals
    public static ModelParameter fromJson(JSONObject jsonObject) {
        RepositoryId id = RepositoryId.fromJsonObj(jsonObject, "id");
        UUID uuid = UUID.fromString(jsonObject.getAsString("uuid"));
        String dataType = getStringOrDef(jsonObject, "data_type");
        boolean output = getNumberOrDef(jsonObject, "is_output", 0) != 0;
        String defaultValue = getStringOrDef(jsonObject, "default_value");
        String description = getStringOrDef(jsonObject, "description").replace("\r", "").replace("\n", " ");
        final Object o = jsonObject.get("semtype");
        List<String> semTypes;
        if (o instanceof List) {
            final List<Object> objs = (List<Object>) o;
            semTypes = objs.stream().map(Object::toString).collect(Collectors.toList());
        }
        else {
            final String semtypeStr = getStringOrDef(jsonObject, "semtype");
            semTypes = Arrays.asList(semtypeStr.split("\\s+"));
        }

        boolean dynamic = getNumberOrDef(jsonObject, "is_static", 1) == 0;

        boolean mandatory = getNumberOrDef(jsonObject, "is_mandatory", 1) != 0;
        String name = getStringOrDef(jsonObject, "name");
        String range = getStringOrDef(jsonObject, "data_range");
        String unit = getStringOrDef(jsonObject, "unit");
        final ModelParameter param = ModelParameter.builder()
                .id(id)
                .uuid(uuid)
                .name(name)
                .output(output)
                .description(description)
                .mandatory(mandatory)
                .defaultValue("".equals(defaultValue) ? null : defaultValue)
                .dynamic(dynamic)
                .dataType(dataType)
                .unit(unit)
                .range(range)
                .semTypes(semTypes)
                .build();
        return param;
    }
}