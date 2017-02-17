package gr.forth.ics.cbml.chic.hme.server.modelrepo;

public enum WorkflowKind {
    T2FLOW, XMML, OTHER;

    public static WorkflowKind fromString(String text) {
        if ("t2flow".equalsIgnoreCase(text))
            return T2FLOW;
        if ("xmml".equalsIgnoreCase(text))
            return XMML;
        return OTHER;
    }

    public String toFileKind() {
        if (this == T2FLOW)
            return "t2flow";
        if (this == XMML)
            return "xmml";
        return "other";
    }
}