CREATE TABLE ${0}.third
     ( name         name
     , indx         integer
     , first        bigint
     , second       uuid
     , bool         boolean
     , time         timestamp with time zone
     , unsupported  timestamp without time zone

     , CONSTRAINT pk_third PRIMARY KEY (name, indx)
     )