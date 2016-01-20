pgdbsync
===========

[![Build Status](https://travis-ci.org/gonzalo123/pgdbsync.svg?branch=master)](https://travis-ci.org/gonzalo123/pgdbsync)

pgdbsync allows us to track the differences in the database structure between different databases. It also create the needed script to synchronize the servers and even to run the script.

The usage of pgdbsync command line script is the following one:
```
 -c [config]
 -f [from database]
 -t [to database]
 -a [action: diff | summary | run] 
```

## usage examples

### Summary
```
./pgdbsync -s web -f devel -t prod -a summary
HOST : production :: prod1
--------------------------------------------
function
 create :: WEB.hello(varchar)
tables
 create :: WEB.test
view
 create :: WEB.testview

[OK]  end process
```

### Creating diff script
```
./pgdbsync -s wf -f devel -t prod -a diff
HOST : production :: prod1
--------------------------------------------
CREATE OR REPLACE FUNCTION web.hello(item character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
DECLARE
BEGIN
 return "Hi " || item;
END;
$function$

CREATE TABLE web.test(
 test_name character NOT NULL,
 test_id integer NOT NULL,
 test_date timestamp without time zone NOT NULL,
 CONSTRAINT pk_test PRIMARY KEY (test_date)
)
TABLESPACE web;
ALTER TABLE web.test OWNER TO user;

CREATE OR REPLACE VIEW web.testview AS
 SELECT test.test_name, test.test_id, test.test_date FROM web.test WHERE (test.test_name ~~ 't%'::text);;
ALTER TABLE web.testview OWNER TO user;
[OK]  end process
executing diff script
./pgdbsync -s web -f devel -t prod -a run
HOST : production :: prod1
----------------------------------
﻿
[OK]  end process
creating diff script again
./pgdbsync -s wf -f devel -t prod1 -a diff

HOST : prododuction :: prod1
--------------------------------------------

drop function web.hello(varchar);

DROP TABLE web.test;

drop view web.testview;

[OK]  end process
```