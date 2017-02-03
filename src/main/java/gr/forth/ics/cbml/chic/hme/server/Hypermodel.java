package gr.forth.ics.cbml.chic.hme.server;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

@Data
@Builder
public class Hypermodel {
    UUID uuid;
    long version;
    String name;
    String description;
    Instant createdAt;
    Instant updatedAt;
    JSONObject graph;
    boolean isFrozen;
    @Singular List<Long> allVersions;


    public String versionStr()
    {
        final int i = allVersions.size();
        if (isFrozen)
            return "1." + i;
        return "0."+i;
    }

    public long mostRecentVersion() {
        return allVersions.stream().max(Comparator.naturalOrder()).orElse(version);
    }
    public String uri()
    {
        return "/hypermodels/" + uuid;
    }
    public String versionUri(long version)
    {
        return uri() + "/" + version;
    }

    public JSONObject toJson()
    {
        final JSONObject js = new JSONObject();
        js.put("uuid", uuid.toString());
        js.put("version", "" + version);
        js.put("modelRepoVersion", versionStr());
        js.put("most_recent_version", ""+mostRecentVersion());
        js.put("frozen", isFrozen);
        js.put("title", name);
        js.put("description", description);
        // js.put("canvas", canvas);
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
        js.put("created_at", dateTimeFormatter.format(createdAt));
        js.put("updated_at", dateTimeFormatter.format(updatedAt));
        js.put("graph", graph);

        final JSONObject links = new JSONObject();
        links.put("self", versionUri(version));

        final JSONArray verLinks = new JSONArray();
        allVersions.forEach(ver -> verLinks.add(versionUri(ver)));
        links.put("versions", verLinks);
        js.put("_links", links);
        return js;
    }

}
