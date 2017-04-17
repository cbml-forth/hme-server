package gr.forth.ics.cbml.chic.hme.server.execution;

import gr.forth.ics.cbml.chic.hme.server.modelrepo.RepositoryId;

import java.time.Instant;

/**
 * Created by ssfak on 31/1/17.
 */
public class TrFile {

    private final RepositoryId id;
    private final String kind;
    // private final Date createdOn;
    private final Instant createdOn;

    public TrFile(final RepositoryId id, final String kind, final Instant createdOn)
    {
        this.id = id;
        this.kind = kind;
        this.createdOn = createdOn; //DatatypeConverter.parseDateTime(createdOn).getTime();
    }

    public RepositoryId getId() {
        return id;
    }

    public String getKind() {
        return kind;
    }

    public Instant getCreatedOn() {
        return createdOn;
    }

    public boolean isReport() {
        return "report".equalsIgnoreCase(this.kind);
    }

    public boolean isInput() {
        return "baclava".equalsIgnoreCase(this.kind);
    }

    public boolean isOutput() {
        return "zip".equalsIgnoreCase(this.kind);
    }

    @Override
    public String toString() {
        return String.format("<TrFile{id=%s, kind=%s, createdOn:%s}",
                this.id, this.kind, this.createdOn.toString());
    }
}