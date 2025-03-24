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

### Field 371

- **Mapped:** `N`
- **Notes:**
  - Identifies an address associated with the Person. In the existing data, this is largely email addresses (and thus not used by LUX), or residences already found in **370**.

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

### Field 377

- **Mapped:** `N`
- **Notes:**  
  - Identifies the **language** associated with a person or group and **does not map to Linked.art.**

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

### Field 550

- **Mapped:** `N`
- **Notes:**
  - Defines associative relationships between records. In reviewing the data, the association is often too vague to be considered a model-able relationship.

---

### Field 551

- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Residence data point

---

### Field 682

- **Mapped:** `N`
- **Notes:**  
  - Identifies deleted or replaced headings. The **Loader** for FAST will check this subfield and not load records that are deleted.

---

### Field 688

- **Mapped:** `N`
- **Notes:**  
  - This field documents the application history of the heading and **does not map to Linked.art**.

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

### Field 016

- **Mapped:** `N`
- **Notes:**  
  - Control number field, but only repeats **FAST control number** in `001`, which LUX uses for the identifier.

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

### Field 072

- **Mapped:** `N`
- **Notes:**
  - Identifies a subject category code from a controlled vocabulary. However, URIs are not given, and the code string does not reconcile to any of LUX's sources.

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

## Topical

**Maps to:** Linked.art `Type` Class
**Good identifiers for testing:** `1754987`,`826218`,`941961`

---

### Field 688

- **Mapped:** `N`
- **Notes:**  
  - This field documents the application history of the heading and **does not map to Linked.art**.

---

### Field 750 & 710

- **Mapped:** `Y`
- **Subfields used:** `'0','1'`
  - Equivalent relationships to external authorities.

Tag 040: 486766 occurrences
Tag 016: 486765 occurrences
Tag 024: 486765 occurrences
Tag 150: 486765 occurrences
Tag 450: 273688 occurrences
Tag 550: 230156 occurrences
Tag 780: 177206 occurrences
Tag 053: 95585 occurrences
Tag 682: 45913 occurrences
Tag 755: 31026 occurrences
Tag 551: 19721 occurrences
Tag 072: 2046 occurrences
Tag 555: 1352 occurrences
Tag 680: 826 occurrences
Tag 751: 730 occurrences
Tag 748: 631 occurrences
Tag 700: 569 occurrences
