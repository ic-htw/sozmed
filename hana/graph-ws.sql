set schema sozmed;
drop graph workspace GWS_SOZMED;

create graph workspace GWS_SOZMED
  vertex table TagClass
    key id
    label 'TagClass'
  vertex table Tag
    key id
    label 'Tags'
  edge table Tag
    source typetagclassid references Tag
    target id references TagClass
    label 'HAS_TYPE'
    KEY id