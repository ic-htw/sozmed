ALTER TABLE City ADD FOREIGN KEY (PartOfCountryId) REFERENCES Country(Id);
ALTER TABLE Company ADD FOREIGN KEY (LocationPlaceId) REFERENCES Country(id);
ALTER TABLE University ADD FOREIGN KEY (LocationPlaceId) REFERENCES City(id);

ALTER TABLE Tag ADD FOREIGN KEY (TypeTagClassId) REFERENCES TagClass(id);
ALTER TABLE TagClass ADD FOREIGN KEY (SubclassOfTagClassId) REFERENCES TagClass(id);

ALTER TABLE Person ADD FOREIGN KEY (LocationCityId) REFERENCES City(id);

ALTER TABLE Person_hasInterest_Tag ADD FOREIGN KEY (TagId) REFERENCES Tag(id);
ALTER TABLE Person_hasInterest_Tag ADD FOREIGN KEY (PersonId) REFERENCES Person(id);

ALTER TABLE Person_studyAt_University ADD FOREIGN KEY (UniversityId) REFERENCES University(Id);
ALTER TABLE Person_studyAt_University ADD FOREIGN KEY (PersonId) REFERENCES Person(id);

ALTER TABLE Person_workAt_Company ADD FOREIGN KEY (CompanyId) REFERENCES Company(id);
ALTER TABLE Person_workAt_Company ADD FOREIGN KEY (PersonId) REFERENCES Person(id);

ALTER TABLE Person_knows_Person ADD FOREIGN KEY (Person1Id) REFERENCES Person(id);
ALTER TABLE Person_knows_Person ADD FOREIGN KEY (Person2Id) REFERENCES Person(id);

ALTER TABLE Person_likes_Message ADD FOREIGN KEY (PersonId) REFERENCES Person(id);
ALTER TABLE Person_likes_Message ADD FOREIGN KEY (MessageId) REFERENCES Message(id);

ALTER TABLE Forum_hasTag_Tag ADD FOREIGN KEY (TagId) REFERENCES Tag(id);
ALTER TABLE Forum_hasTag_Tag ADD FOREIGN KEY (ForumId) REFERENCES Forum(id);

ALTER TABLE Message ADD FOREIGN KEY (LocationCountryId) REFERENCES Country(id);
ALTER TABLE Message ADD FOREIGN KEY (CreatorPersonId) REFERENCES Person(id);
ALTER TABLE Message ADD FOREIGN KEY (ContainerForumId) REFERENCES Forum(id);
ALTER TABLE Message ADD FOREIGN KEY (ParentMessageId) REFERENCES Message(id);

ALTER TABLE Message_hasTag_Tag ADD FOREIGN KEY (TagId) REFERENCES Tag(id);
ALTER TABLE Message_hasTag_Tag ADD FOREIGN KEY (MessageId) REFERENCES Message(id);

ALTER TABLE Forum_hasMember_Person ADD FOREIGN KEY (ForumId) REFERENCES Forum(id);
