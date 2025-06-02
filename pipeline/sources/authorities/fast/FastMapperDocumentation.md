# FAST to Linked.art Mapping

Based on the occurrences of MARC21 fields across the available FAST XML files, the Mapper attempts to map any that occur more than 500 times. However, not all fields with more than 500 occurrences are mapped, based on considerations below. Please note these mappings are based on what is found in the existing data, not on  MARC cataloging rules.

## Personal & Corporate Common Fields

---

### Field 046

- **Mapped:** `Y`
- **Subfields used:** `'f','g'`
- **Usage:**
  - **'f'**
    - Preferred birth/formation date
  - **'g'**
    - Preferred death/dissolution date

---

### Field 368

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Classification

---

### Field 370

- **Mapped:** `Y`
- **Subfields used:** `'a','b','c','e'`
- **Usage:**
  - **'a'**
    - Birth location
  - **'b'**
    - Death location
  - **'c'**
    - Residence data point
  - **'e'**
    - Residence data point

---

### Field 372

- **Mapped:** `Y`
- **Subfields used:** `'a','s','t'`
- **Usage:**
  - **'a'**
    - Field of activity
  - **'s'**
    - Activity start
  - **'t'**
    - Activity end

---

### Field 373

- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Name of Group membership
  - **'0'**
    - URI for Group membership

---

### Field 374

- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Name of occupation
  - **'0'**
    - URI of occupation

---

### Field 500

- **Mapped:** `Y`
- **Subfields used:** `'a'`,`'i'`
- **Usage:**
  - **'i','a'**
    - (combined) Biographical note

---

### Field 510

- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Name of Group membership
  - **'0'**
    - (reformatted) URI for Group membership

---

### Field 551

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Residence data point

---

### Field 700

- **Mapped:** `Y`
- **Subfields used:** `a`,`0`,`1`
- **Usage:**
  - **'a'**
    - See Personal mapping.
  - **'0' & '1'**
    - Equivalent relationships to external authorities.

---

### Field 710

- **Mapped:** `Y`
- **Subfields used:** `'a', 'b', '0'`
- **Usage:**
  - **'a' & 'b'**
    - See Group mapping
  - **'0' & '1'**
    - Equivalent relationships to external authorities.

---

## Personal

**Maps to:** Linked.art `Person` Class
**Good identifiers for testing:** `269901`, `1462822`, `1497726`

---

### Field 100

- **Mapped:** `Y`
- **Subfields used:** `'a', 'd'`
- **Usage:**
  - **'a'**  
    - Preferred primary name
  - **'d'**
    - Alternate birth or death date

---

### Field 700

- **Mapped:** `Y`
- **Subfields used:** `a`,`0`,`1`
- **Usage:**
  - **'a'**
    - Alternate primary name
    - Preferred alternate name
  - **'0'** and **'1'**
    - See common fields mapping.

---

### Field 400

- **Mapped:** `Y`
- **Subfields used:** `'a', 'q', 'd'`
- **Usage:**
  - **'a'** and **'q'**  
    - Alternate primary name
    - Preferred alternate name  
  - **'d'**
    - Alternate birth or death date

---

### Field 378

- **Mapped:** `Y`
- **Subfields used:** `'a','q'`
- **Usage:**
  - **'a'**
    - Alternate primary name
    - Preferred alternate name
  - **'q'**
    - Alternate primary name
    - Preferred alternate name

---

### Field 450 and 410

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Alternate primary name
    - Preferred alternate name

---

### Field 375

- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Name of gender
  - **'0'**
    - URI for gender

---

## Corporate

**Maps to:** Linked.art `Group` Class
**Good identifiers for testing:** `544497`,`1892632`,`520318`

---

### Field 110

- **Mapped:** `Y`
- **Subfields used:** `'a', 'b'`
- **Usage:**
  - **'a'**
    - Preferred primary name, part 1
  - **'b'**
    - Preferred primary name, part 2

---

### Field 410

- **Mapped:** `Y`
- **Subfields used:** `'a', 'b'`
- **Usage:**
  - **'a'**
    - Alternate primary name, part 1
    - Preferred alternate name, part 1
  - **'b'**
    - Alternate primary name, part 2
    - Preferred alternate name, part 2

---

### Field 411

- **Mapped:** `Y`
- **Subfields used:** `'a'
- **Usage:**
  - **'a'**
    - Alternate primary name, part 1
    - Preferred alternate name, part 1

---

### Field 710

- **Mapped:** `Y`
- **Subfields used:** `'a', 'b', '0'`
- **Usage:**
  - **'a'**
    - Alternate primary name, part 1
    - Preferred alternate name, part 1
  - **'b'**
    - Alternate primary name, part 2
    - Preferred alternate name, part 2
  - **'0'**
    - Equivalent relationships to external authorities.

---

## Topical & FormGenre

**Maps to:** Linked.art `Type` Class
**Good identifiers for testing:** `1754987`,`826218`,`941961` (Topical), `1423715`,`1423835`,`1692824` (FormGenre)

---

### Field 155

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Preferred primary name

---

### Field 455

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Preferred alternate name
    - Alternate primary name

---

### Field 555

- **Mapped:** `Y`. Unlike 555 in Topical terms, this relationship does consistly appear to be that of **broader**.
- **Subfields used:** `'a','0`
- **Usage:**
  - **'a'**
    - Name of broader GenreForm term.
  - **'0'**
    - FAST identifier of broader GenreForm term.

---

### Field 755

- **Mapped:** `Y`
- **Subfields used:** `'0'`
- **Usage:**
  - **'0'**
    - LCGFT identifier of equivalent GenreForm term.

---

### Field 750

- **Mapped:** `Y`
- **Subfields used:** `'0'`
  - Equivalent relationship to LCSH.

---

### Field 710, 750 & 751

- **Mapped:** `Y`
- **Subfields used:** `'0','1'`
  - Equivalent relationships to external authorities.

---

### Field 150

- **Mapped:** `Y`
- **Subfields used:** `'a', 'x'`
- **Usage:**
  - **'a'**
    - Preferred primary name, part 1
  - **'x'**
    - Preferred primary name, part 2

---

### Field 450

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Preferred alternate name
    - Alternate primary name

---

### Field 550

- **Mapped:** `Y`
- **Subfields used:** `'g'`
- **Usage:**
  - **'g'**
    - Broader term
  - **'0'**
    - Broader term FAST ID

---

### Field 680

- **Mapped:** `Y`
- **Subfields used:** `'i'`
- **Usage:**
  - **'i'**
    - Descriptive note

---

## Geographic

**Maps to:** Linked.art `Place` Class. 
**Good identifiers for testing:** `1202829`,`1202950`,`1204195`

---

### Field 751

- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Alternate primary name
    - Preferred alternate name
  - **'0'**
    - Equivalent to external authority.

---

### Field 151

- **Mapped:** `Y`
- **Subfields used:** `'a','z``
- **Usage:**
  - **'a','z'**
    - Preferred primary name

---

### Field 670

- **Mapped:** `Y`
- **Subfields used:** `'b'`
- **Usage:**
  - **'b'**
    - Latitude and Longitude as a string. The mapper converts this to WKT and assigns it as defined_by.

---

### Field 550 & 368

- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Classification name, used for reconciliation if URI does not exist.
  - **'0'**
    - Classification URI.

---

### Field 410

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Preferred alternate name
    - Alternate primary name

---

### Field 370

- **Mapped:** `Y`
- **Subfields used:** `'c','e','f','0'`
- **Usage:**
  - **'c','e','f'**
    - Broader Places for this entity. If no URI is available in |0, the mapper attempts to reconcile this string to NAF.
  - **'0'**
    - URI of the broader Place, if available.

---

## Chronological

**Maps to:** Linked.art `Period` Class.
**Good identifiers for testing:** `1352135`,`1710758`,`2010878`

---

### Field 448

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Preferred primary name

---

### Field 148

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Timespan of Period.

---

## Event & Meeting

**Maps to:** Linked.art `Event` Class.
**Good identifiers for testing:** `1709869`,`827161`,`922029`,`1025741`,`1153111`,`1405209`


---

### Field 410, 411 & 447

- **Mapped:** `Y`
- **Subfields used:** `'a','b','d'`
- **Usage:**
  - **'a','b','d'**
    - Alternate primary name
    - Preferred alterate name

---

### Field 147 & 111

- **Mapped:** `Y`
- **Subfields used:** `'a','n','d'`
- **Usage:**
  - **'a','n','d'**
    - Preferred primary name

---

### Field 551 & 370

- **Mapped:** `Y`
- **Subfields used:** `'a','c','e','f','0'`
- **Usage:**
  - **'a','c','e','f'**
    - Name of Place where activity took place. Used for reconciliation if URI does not exist.
  - **'0'**
    - URI of Place where activity took place.

---

### Field 046 & 748

- **Mapped:** `Y`
- **Subfields used:** `'s','t','a'`
- **Usage:**
  - **'s','a'**
    - Start date of activity.
  - **'t'**
    - End date of activity.

---

### Field 750, 751, 711

- **Mapped:** `Y`
- **Subfields used:** `'0','1'`
- **Usage:**
  - **'0'**
    - Equivalent to LCSH
  - **'1'**
    - Equivalent to other external authorities

---

### Field 547

- **Mapped:** `Y`
- **Subfields used:** `'a','c','d','0'`
- **Usage:**
  - **'a','c','d'**
    - Name parts of broader Activity
  - **'0'**
    - Equivalent broader Activity

---

### Field 550 & 368

- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Classification name, used for reconciliation if URI does not exist.
  - **'0'**
    - Classification URI.

---

## Unmapped Fields

---

### Field 024

- **Mapped:** `N`
- **Notes:**  
  - Identifies **standard identifiers**.  
  - LUX uses control number `001` to build the identifier/URI.

---

### Field 040

- **Mapped:** `N`
- **Notes:**  
  - Identifies the **institution** creating, modifying, or maintaining the bibliographic record.  
  - This is evident in **LUX via the URI** and not explicitly mapped.

---

### Field 016

- **Mapped:** `N`
- **Notes:**  
  - Control number field, but only repeats **FAST control number** in `001`, which LUX uses for the identifier.

---

### Field 555

- **Mapped:** `N`
- **Notes:**  
  - A related topical term that is too broad to be a classification.

---

### Field 043

- **Mapped:** `N`
- **Notes:**  
  - A string-based geographic area code and **does not map to Linked.art.**

---

### Field 688

- **Mapped:** `N`
- **Notes:**  
  - This field documents the application history of the term and **does not map to Linked.art**.

---

### Field 046

- **Mapped:** `N`
- **Notes:**
  - Unmapped only on Place Class.

---

### Field 072

- **Mapped:** `N`
- **Notes:**
  - Identifies a subject category code. However, URIs are not given, and the code string does not reconcile to any of LUX's sources.

---

### Field 682

- **Mapped:** `N`
- **Notes:**  
  - Identifies deleted or replaced terms. The **Loader** for FAST will check this subfield and not load records that are deleted.

---

### Field 551

- **Mapped:** `N`
- **Notes:**
  - Umapped on Place and Type Classes. Relationship is either too broad or does not exist in Linked.art model.

---

### Field 034

- **Mapped:** `N`
- **Notes:**
  - Repeats geographic information structured as 4 strings, NWES. However, there are more entries in 670 and they completely cover the information available in 034.

---

### Field 371

- **Mapped:** `N`
- **Notes:**
  - Identifies an address associated with a Person. In the existing data, this is largely email addresses (and thus not used by LUX), or residences already found in **370**.

---

### Field 377

- **Mapped:** `N`
- **Notes:**  
  - Identifies the **language** associated with a person or group and **does not map to Linked.art.**

---

### Field 550

- **Mapped:** `N`
- **Notes:**
  - Defines associative relationships between records. Unmapped only for person and group, as the association is too vague to be model-able.

---

### Field 670

- **Mapped:** `N`
- **Notes:**
  - Identifies a reference to a bibliographic resource that describes the Person and **is not used by LUX.**

---

### Field 053

- **Mapped:** `N`
- **Notes:**  
  - Identifies works associated with an entity by LCC number and **does not map to Linked.art**.

---

### Field 451

- **Mapped:** `N`
- **Notes:** 
  - Documents See From Tracing-Geographic Name. Subfield a is a more specific place for the entity, e.g. "Alaska". However, the primary name of the Geographic entity should be maintained as it is in 151, for reconciliation purposes. And Places in Linked.art do not have locations of their own, as they are a location. Thus this **does not map to Linked.art.**

---

### Field 780

- **Mapped:** `N`
- **Notes:**
  - Identifies preceding entry, and **does not map to Linked.art.**

---

### Field 748

- **Mapped:** `N`
- **Notes:**  
  - Unmapped on Topical headings. Timespans related to topical term, which **does not map to Linked.art.**

---

### Field 700

- **Mapped:** `N`
- **Notes:**  
  - Unmapped on Topical headings. Person related to topical term, which **does not map to Linked.art.**

---