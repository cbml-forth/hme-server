package gr.forth.ics.cbml.chic.hme.server.execution;

/**
 * Created by ssfak on 31/1/17.
 */
import net.minidev.json.JSONObject;

public class Trial {
    public final String id;
    public String model_id;
    private String description;

    public Trial(final String id) {
        this(id, null);
    }

    public Trial(final String id, final String model_id) {
        this.id = id;
        this.model_id = model_id;
    }
    public JSONObject toJson() {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", this.id);
        jsonObject.put("model_id", this.model_id);
        jsonObject.put("description", this.description);
        return jsonObject;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}