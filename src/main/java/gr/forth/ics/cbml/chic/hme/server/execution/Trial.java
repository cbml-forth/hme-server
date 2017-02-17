package gr.forth.ics.cbml.chic.hme.server.execution;

/**
 * Created by ssfak on 31/1/17.
 */
import gr.forth.ics.cbml.chic.hme.server.modelrepo.RepositoryId;
import net.minidev.json.JSONObject;

public class Trial {
    public final RepositoryId id;
    public RepositoryId model_id;
    private String description;

    public Trial(final RepositoryId id) {
        this(id, null);
    }

    public Trial(final RepositoryId id, final RepositoryId model_id) {
        this.id = id;
        this.model_id = model_id;
    }
    public JSONObject toJson() {
        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", this.id.getId());
        jsonObject.put("model_id", this.model_id.getId());
        jsonObject.put("description", this.description);
        return jsonObject;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}