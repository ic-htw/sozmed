drop graph workspace GWS_SOZMED;

create graph workspace GWS_SOZMED
  vertex table TagClass
    key id
    label 'TagClass'
  edge table TagClass
    source subclassoftagclassid references TagClass
    target id references TagClass
    label 'IS_SUBCLASS_OF'
    KEY id