--
-- Adds the new active column to the ngas_subscribers table,
-- defaulting to 1 to make sure all present subscribers are considered active
--
ALTER TABLE ngas_subscribers ADD COLUMN active smallint NOT NULL DEFAULT 1 CHECK (active IN (0, 1));