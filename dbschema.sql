--- CREATE USER hme WITH PASSWORD 'hme';
--- CREATE DATABASE hme OWNER hme;


CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;

CREATE TABLE hypermodels (
    hypermodel_uid UUID PRIMARY KEY,
    user_id text NOT NULL, -- Creator
    created TIMESTAMP WITH TIME ZONE DEFAULT now() --timezone('UTC'::text, now()) NOT NULL
    , updated TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE hypermodel_versions (
   hypermodel_version BIGSERIAL PRIMARY KEY,
   hypermodel_uid UUID NOT NULL,
   frozen  BOOLEAN DEFAULT (FALSE), -- a frozen version can be used by CRAF...
   version_created TIMESTAMP WITH TIME ZONE DEFAULT now(), --timezone('UTC'::text, now()) NOT NULL,
   title text NOT NULL DEFAULT (''),
   description text NOT NULL DEFAULT (''),
   svg_content text NOT NULL, -- svg content of the browser
   json_content text NOT NULL, -- JSON string in the joint.js format
   graph_content jsonb NOT NULL, -- JSON data, contains the graph in my own format

   CONSTRAINT hypermodel_versions_uid_fk FOREIGN KEY(hypermodel_uid)
   REFERENCES hypermodels(hypermodel_uid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX hypermodel_versions_hypermodel_uid ON hypermodel_versions(hypermodel_uid);


--Some useful views:


CREATE VIEW recent_versions_vw AS
SELECT hypermodel_uid,
       MAX(hypermodel_version) most_recent_version,
       MAX(version_created) as last_update,
       ARRAY_AGG(hypermodel_version ORDER BY hypermodel_version DESC) as versions,
       ARRAY_AGG(hypermodel_version ORDER BY hypermodel_version DESC) FILTER (WHERE frozen = TRUE) as frozen_versions
FROM hypermodel_versions
GROUP BY hypermodel_uid;


CREATE VIEW hypermodel_versions_vw AS
SELECT H.user_id, H.hypermodel_uid,
       V.hypermodel_version, V.frozen,
       H.created,
       V.version_created, V.title, V.description,
       R.most_recent_version, R.last_update, R.versions, R.frozen_versions,
       V.json_content, V.svg_content,
       V.graph_content::text
FROM hypermodels H JOIN hypermodel_versions V USING (hypermodel_uid)
     JOIN recent_versions_vw R USING (hypermodel_uid);


--------------- Keep the models used in each version in a separate TABLE
------- Since only INSERTs are performed in the hypermodel_versions (base) table...

-- I don't want the built-in materialized view support of Postgres, since then I would need to
-- REFRESH MATERIALIZED VIEW on every insert. But this REFRESH recomputes the whole materialized
-- table from scratch. So instead I use a common table that is updated on INSERT trigger on the
-- `hypermodel_versions` base table. This is a better solution since we don't perform UPDATEs on
-- the base table

CREATE TABLE hypermodel_versions_models (
    hypermodel_uid UUID NOT NULL REFERENCES hypermodels(hypermodel_uid) ON DELETE CASCADE,
    hypermodel_version BIGINT NOT NULL REFERENCES hypermodel_versions(hypermodel_version) ON DELETE CASCADE,
    model_uuid UUID NOT NULL
);
CREATE INDEX hypermodel_versions_models_version_idx ON hypermodel_versions_models(hypermodel_version);
CREATE INDEX hypermodel_versions_models_uid_idx ON hypermodel_versions_models(hypermodel_uid);

CREATE OR REPLACE FUNCTION create_trig_refresh_hypermodel_versions_models() RETURNS trigger AS
$$
BEGIN
    INSERT INTO hypermodel_versions_models(hypermodel_uid, hypermodel_version, model_uuid)
    SELECT distinct NEW.hypermodel_uid, NEW.hypermodel_version, (js#>>'{kind,id}') :: UUID
    FROM json_array_elements(json_extract_path(NEW.graph_content::json, 'nodes'::text)) as x(js);
    RETURN NULL;
END;
$$
LANGUAGE plpgsql ;

CREATE TRIGGER trig_refresh_hypermodel_versions_models
AFTER INSERT ON hypermodel_versions
    FOR EACH ROW EXECUTE PROCEDURE create_trig_refresh_hypermodel_versions_models();