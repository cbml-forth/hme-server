package gr.forth.ics.cbml.chic.hme.server.modelrepo;

import lombok.Value;
import net.minidev.json.JSONObject;

@Value
public class RepositoryId {
    final public long id;

    public long toJSON()
    {
        return this.id;
    }
    public static RepositoryId fromJsonObj(final JSONObject o, final String field)
    {
        return new RepositoryId(o.getAsNumber(field).longValue());
    }
    @Override
    public String toString() {
        return Long.toString(this.id);
    }
}
