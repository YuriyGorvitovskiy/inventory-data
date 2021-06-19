ALTER TABLE ${0}.third
      ADD CONSTRAINT fk_third_first  FOREIGN KEY (first)  REFERENCES ${0}.first  (id)
    , ADD CONSTRAINT fk_third_second FOREIGN KEY (second) REFERENCES ${0}.second (id)
