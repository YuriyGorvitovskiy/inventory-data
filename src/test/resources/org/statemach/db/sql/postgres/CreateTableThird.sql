CREATE TABLE ${0}.third
     ( name         name
     , indx         integer
     , first        bigint
     , second       uuid
     , bool         boolean
     , time         timestamp without time zone
     , unsupported  timestamp with time zone

     , CONSTRAINT pk_third PRIMARY KEY (name, indx)
     )