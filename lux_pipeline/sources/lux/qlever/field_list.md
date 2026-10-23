
# Triples

## All Classes
- [X] name
- [X] primaryName
- [X] sortName
- [X] classification
- [X] hasDigitalImage
- [X] identifier
- [X] memberOf
- [X] recordType
- [X] text

## Agent
- [X] startAt = fou
- [X] startDate = startOfAgentBeginning / endOfAgentBeginning
- [X] endAt = placeOfAgentEnding
- [X] endDate = startOfAgentEnding / endOfAgentEnding
- [X] foundedBy = agentOfAgentBeginning
- [X] gender = gender
- [X] nationality = nationality
- [X] occupation = occupation
- [X] professionalActivity = typeOfAgentActivity
- [X] activeAt = placeOfAgentActivity
- [X] activeDate = startofAgentActivity / endOfAgentActivity

## Concept
- [X] broader = broader

## Event
- [X] carriedOutBy = agentOfEvent
- [X] tookPlaceAt = placeOfEvent
- [X] used = eventUsedSet 
- [ ] ?? = causeOfEvent
- [X] endDate
- [X] startDate



## Item
- [ ] isOnline
- [X] producedAt = placeOfItemBeginning
- [X] producedBy = agentOfItemBeginning
- [X] producedUsing = typeOfItemBeginning
- [X] productionInfluencedBy = agentInfluenceOfItemBeginning
- [X] producedDate = startOfItemBeginning / endOfItemBeginning
- [X] dimension
- [X] depth
- [X] height
- [X] width
- [X] encounteredAt = placeOfItemEncounter
- [X] encounteredDate = startOfItemEncounter / endOfItemEncounter
- [X] encounteredBy = agentOfItemEncounter
- [X] carries = carries
- [X] material = material
+ weight
I
## Place
- [X] partOf = placePartOf

## Set
- [X] aboutConcept = setAboutConcept
- [X] aboutEvent = setAboutEvent
- [X] aboutItem = setAboutItem
- [X] aboutAgent = setAboutAgent
- [X] aboutPlace = setAboutPlace
- [X] aboutWork = setAboutWork
- [X] createdAt = placeOfSetBeginning
- [X] createdBy = agentOfSetBeginning
- [X] creationCausedBy = causeOfSetBeginning
- [X] createdDate = startOfSetBeginning / endOfSetBeginning
- [X] curatedBy = setCuratedBy
- [X] publishedAt = placeOfSetPublication
- [X] publishedBy = agentOfSetPublication
- [X] publishedDate = startOfSetPublication / endOfSetPublication

## Work
- [X] aboutConcept = workAboutConcept
- [X] aboutEvent = workAboutEvent
- [X] aboutItem = workAboutItem
- [X] aboutAgent = workAboutAgent
- [X] aboutPlace = workAboutPlace
- [X] aboutWork = workAboutWork
- [X] createdAt = placeOfWorkBeginning
- [X] createdBy = agentOfWorkBeginning
- [X] creationCausedBy = causeOfWorkBeginning
- [X] creationInfluencedBy = agentInfluenceOfWorkBeginning
- [X] publishedAt = placeOfWorkPublication
- [X] publishedBy = agentOfWorkPublication
- [X] language = workLanguage
- [X] partOfWork = workPartOf
- [X] createdDate = startOfWorkBeginning / endOfWorkBeginning
- [X] publishedDate = startOfWorkPublication / endOfWorkPublication
- [ ] isOnline
- [ ] isPublicDomain




# Inverses

## Agent
- [X] createdSet = ^agentOfSetBeginning
- [X] produced = ^agentOfItemBeginning
- [X] created = ^agentOfWorkBeginning
- [X] carriedOut = ^eventCarriedOutBy
- [X] curated = ^setCuratedBy
- [X] encountered = ^agentOfItemEncounter
- [X] founded = ^agentOfAgentBeginning
- [X] memberOfInverse = ^agentMemberOfGroup
- [X] influencedProduction = ^agentInfluenceOfItemBeginning
- [X] influencedCreation = ^agentInfluenceOfWorkBeginning
- [X] publishedSet = ^agentOfSetPublication
- [X] published = ^agentOfWorkPublication
- [X] subjectOfSet = ^setAboutAgent
- [X] subjectOfWork = ^workAboutAgent

## Concept
- [X] classificationOfSet = ^setClassification
- [X] classificationOfConcept = ^conceptClassification
- [X] classificationOfEvent = ^eventClassification
- [X] classificationOfItem = ^itemClassification
- [X] classificationOfAgent = ^agentClassification
- [X] classificationOfPlace = ^placeClassification
- [X] classificationOfWork = ^workClassification
- [X] genderOf = ^gender
- [X] languageOf = ^workLanguage
- [X] --- = ^setLanguage
- [X] materialOfItem = ^material
- [X] narrower = ^broader
- [X] nationalityOf = ^nationality
- [X] occupationOf = ^occupation
- [X] professionalActivityOf = ^typeOfAgentActivity
- [X] subjectOfSet = ^setAboutConcept
- [X] subjectOfWork = ^workAboutConcept
- [X] usedToProduce = ^typeOfItemBeginning

## Event
- [X] causedCreationOf = ^causeOfWorkBeginning
- [X] subjectOfSet = ^setAboutEvent
- [X] subjectOfWork = ^workAboutEvent

## Item
- [X] subjectOfSet = ^setAboutItem
- [X] subjectOfWork = ^workAboutItem

## Place
- [X] activePlaceOfAgent = ^placeOfAgentActivity
- [X] startPlaceOfAgent = ^placeOfAgentBeginning
- [X] producedHere = ^placeOfItemBeginning
- [X] createdHere = ^placeOfWorkBeginning
- [X] endPlaceOfAgent = ^placeOfAgentEnding
- [X] encounteredHere = ^placeOfItemEncounter
- [X] placeOfEvent = ^placeOfEvent
- [X] setPublishedHere = ^placeOfSetPublication
- [X] publishedHere = ^placeOfWorkPublication
- [X] subjectOfSet = ^setAboutPlace
- [X] subjectOfWork = ^workAboutPlace

## Set
- [X] containingSet = ^setMemberOfSet
- [X] containingItem = ^itemMemberOfSet
- [X] usedForEvent = ^eventUsedSet


## Work
- [X] subjectOfSet = ^setAboutWork
- [X] subjectOfWork = ^workAboutWork
- [X] carriedBy = ^carries
- [X] containsWork = ^partOfWork
