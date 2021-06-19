ALTER TABLE ${0}.second
      ADD CONSTRAINT fk_second_first      FOREIGN KEY (first)                  REFERENCES ${0}.first  (id)
    , ADD CONSTRAINT fk_second_second_one FOREIGN KEY (one)                    REFERENCES ${0}.second (id)
    , ADD CONSTRAINT fk_second_second_two FOREIGN KEY (two)                    REFERENCES ${0}.second (id)
    , ADD CONSTRAINT fk_second_third      FOREIGN KEY (third_name, third_indx) REFERENCES ${0}.third  (name, indx)