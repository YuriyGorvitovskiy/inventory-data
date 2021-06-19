CREATE TABLE ${0}.first
     ( id           bigint
     , second       uuid
     , third_name   name
     , third_indx   integer
     , fixed        character (256)
     , varying      character varying(256)
     , unlimited    text
     , search       tsvector    GENERATED ALWAYS AS (to_tsvector('english'
                                                    ,  coalesce(fixed, '') || ' '
                                                    || coalesce(varying, '') || ' '
                                                    || coalesce(unlimited, ''))) STORED

     , CONSTRAINT pk_first PRIMARY KEY (id)
     )
