package gr.forth.ics.cbml.chic.hme.server.execution;

import java.time.Instant;

/**
 * Created by ssfak on 31/1/17.
 */
public class TrFile {

    private final String id;
    private final String kind;
    // private final Date createdOn;
    private final Instant createdOn;

    public TrFile(final String id, final String kind, final Instant createdOn)
    {
        this.id = id;
        this.kind = kind;
        this.createdOn = createdOn; //DatatypeConverter.parseDateTime(createdOn).getTime();
    }

    public String getId() {
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