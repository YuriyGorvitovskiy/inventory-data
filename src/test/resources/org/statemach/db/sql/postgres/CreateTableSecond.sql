CREATE TABLE ${0}.second
     ( id           uuid
     , first        bigint
     , one          uuid
     , two          uuid
     , third_name   name
     , third_indx   integer
     , double       double precision
     , int          integer
     , short        smallint
     , long         bigint

     , CONSTRAINT pk_second PRIMARY KEY (id)
     )
