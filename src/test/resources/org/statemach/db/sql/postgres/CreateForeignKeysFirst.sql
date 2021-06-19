ALTER TABLE ${0}.first
      ADD CONSTRAINT fk_first_second FOREIGN KEY (second)                 REFERENCES ${0}.second (id)
    , ADD CONSTRAINT fk_first_third  FOREIGN KEY (third_name, third_indx) REFERENCES ${0}.third  (name, indx)