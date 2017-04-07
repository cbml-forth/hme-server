package gr.forth.ics.cbml.chic.hme.server.modelrepo;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Wither;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Value
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
    final boolean isStronglyCoupled; //XXX
    @Singular List<Long> allVersions;
    @Wither
    RepositoryId publishedRepoId;


    public Optional<RepositoryId> getPublishedRepoId() {
        return Optional.ofNullable(this.publishedRepoId);
    }
    public String versionStr()
    {
        final String major = isFrozen ? "1" : "0";
        final int minor = allVersions.size();
        return major + "."+minor;
    }

    public long mostRecentVersion() {
        return allVersions.stream().max(Comparator.naturalOrder()).orElse(version);
    }
    public String uri()
    {
        return "/api/hypermodels/" + uuid;
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
        js.put("isStronglyCoupled", isStronglyCoupled);
        js.put("title", name);
        js.put("description", description);
        // js.put("canvas", canvas);
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
        js.put("created_at", dateTimeFormatter.format(createdAt));
        js.put("updated_at", dateTimeFormatter.format(updatedAt));
        js.put("graph", graph);
        if (publishedRepoId != null) {
            final RepositoryId repositoryId = publishedRepoId;
            js.put("publishedRepoId", repositoryId.toJSON());
        }

        final JSONObject links = new JSONObject();
        links.put("self", versionUri(version));

        final JSONArray verLinks = new JSONArray();
        allVersions.forEach(ver -> verLinks.add(versionUri(ver)));
        links.put("versions", verLinks);
        js.put("_links", links);
        return js;
    }

    public Hypermodel withRepoId(RepositoryId id)
    {
        return this.withPublishedRepoId(id);
    }

    public WorkflowKind kind() {
        return this.isStronglyCoupled ? WorkflowKind.XMML : WorkflowKind.T2FLOW;
    }
    public Model toModel()
    {
        assert this.publishedRepoId != null;
        final Model model = new Model(this.publishedRepoId,
                this.name, this.description,
                this.uuid, this.isStronglyCoupled, this.isFrozen,
                Collections.emptyList(), Collections.emptyList());
        return model;

    }
}
