CREATE TABLE od_aggregation_period (
    aggregation_period_id   SERIAL PRIMARY KEY, 
    start_time_period       TIMESTAMPTZ,
    end_time_period         TIMESTAMPTZ,
    calculation_iteration   SMALLINT,
    created_at              TIMESTAMPTZ,
    updated_at              TIMESTAMPTZ
);

CREATE TABLE od_h3 (
    aggregation_period_id INT NOT NULL,
    origin_cell           BIGINT NOT NULL,
    destination_cell      BIGINT NOT NULL,
    h3_level              SMALLINT NOT NULL,
    modality              VARCHAR(30) NOT NULL,
    number_of_trips       INTEGER NOT NULL,
    CONSTRAINT fk_aggregation_period
      FOREIGN KEY(aggregation_period_id) 
	  REFERENCES od_aggregation_period(aggregation_period_id)
	  ON DELETE CASCADE
);

CREATE INDEX od_h3_aggregation_period_id ON od_h3 (aggregation_period_id);
CREATE INDEX od_h3_origin ON od_h3 (origin_cell);
CREATE INDEX od_h3_destination ON od_h3 (destination_cell);
CREATE INDEX od_h3_level ON od_h3 (h3_level);
CREATE INDEX od_h3_modality ON od_h3 (modality);

CREATE TABLE od_geometry (
    aggregation_period_id INT NOT NULL,
    origin_stats_ref      VARCHAR(30) NOT NULL,
    destination_stats_ref VARCHAR NOT NULL,
    modality              VARCHAR(30) NOT NULL,
    number_of_trips       INTEGER NOT NULL,
    CONSTRAINT fk_aggregation_period
      FOREIGN KEY(aggregation_period_id) 
	  REFERENCES od_aggregation_period(aggregation_period_id)
	  ON DELETE CASCADE
);

CREATE INDEX od_geometry_aggregation_period_id ON od_geometry (aggregation_period_id);
CREATE INDEX od_geometry_origin ON od_geometry (origin_stats_ref);
CREATE INDEX od_geometry_destination ON od_geometry (destination_stats_ref);
CREATE INDEX od_geometry_modality ON od_geometry (modality);
