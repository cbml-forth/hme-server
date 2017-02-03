package gr.forth.ics.cbml.chic.hme.server.execution;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ssfak on 31/1/17.
 */

public class Subject {
    private final String id;
    private final String description;
    private String external_id;
    private List<TrFile> files;

    public void setFiles(List<TrFile> files) {
        this.files = files;
    }
    public void addFile(final TrFile file) { this.files.add(file);}

    public Subject(final String id) {
        this(id, null);
    }

    public Subject(final String id, final String description) {
        this.id = id;
        this.description = description;
        this.external_id = "";
        this.files = new ArrayList<>();

        // this.repository = repository;
        // this.token = token;
    }

    public void setExternalId(String external_id) {
        this.external_id = external_id;
    }

    public String getId() {
        return id;
    }

    public String getDescription() {
        return description;
    }

    public String getExternalId() {
        return this.external_id;
    }

    public List<TrFile> getFiles() {
        return files;
    }

}