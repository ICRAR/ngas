# Postgres required triggers for ngas_files
'''
CREATE OR REPLACE FUNCTION notify_update_trigger()
  RETURNS trigger AS
$BODY$

DECLARE

BEGIN
PERFORM
pg_notify(TG_TABLE_NAME, '{"action":"' || TG_OP || '","table":"' || TG_TABLE_NAME || '","row":' || row_to_json(NEW) || '}');
RETURN new;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION notify_update_trigger()
  OWNER TO postgres;


CREATE OR REPLACE FUNCTION notify_delete_trigger()
  RETURNS trigger AS
$BODY$

DECLARE

BEGIN
PERFORM
pg_notify(TG_TABLE_NAME, '{"action":"' || TG_OP || '","table":"' || TG_TABLE_NAME || '","row":' || row_to_json(OLD) || '}');
RETURN old;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION notify_delete_trigger()
  OWNER TO ngas;

CREATE TRIGGER ngas_files_update_trigger
  BEFORE INSERT OR UPDATE
  ON ngas_files
  FOR EACH ROW
  EXECUTE PROCEDURE notify_update_trigger();

CREATE TRIGGER ngas_files_delete_trigger
  BEFORE DELETE
  ON ngas_files
  FOR EACH ROW
  EXECUTE PROCEDURE notify_delete_trigger();
'''

import signal, sys, time, json, select
import psycopg2
import psycopg2.extensions
import psycopg2.extras

def fileArrived(rowdata):
    # print file name and size
    print rowdata['file_id'], rowdata['file_size']

    # Update database(s) etc that the file has arrived.
    # Note: Be warry that this is executed in the same thread context as the connection select,
    # might be a good idea to put this in a threaded message queue.


if __name__ == "__main__":
    # connect to ngas database
    conn = psycopg2.connect(database=None, user=None, host=None, password=None)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor()
    # listen for table changes using trigger
    curs.execute("LISTEN ngas_files;")

    while True:
        if select.select([conn],[],[],5) == ([],[],[]):
            continue
        else:
            conn.poll()
            # process of entries
            while conn.notifies:
                notify = conn.notifies.pop()
                # only action ngas_file changes
                if notify.channel == 'ngas_files':
                    # arrive as a JSON string from psql engine and trigger
                    obj = json.loads(notify.payload) # load into a python dict
                    # action INSERT only
                    if obj['action'] == "INSERT":
                        # handle action
                        fileArrived(obj['row'])
