Based on the occurrences of MARC21 fields across the available FAST XML files, the Mapper attempts to map any that occur more than 500 times. However, not all fields with more than 500 occurrences are mapped, based on considerations below.

## Personal:
**Maps to:** Linked.art `Person` Class

**Good identifiers for testing:** `269901`, `1462822`, `1497726`

### Field 700
- **Occurences:** `1,824,864`
- **Mapped:** `Y`
- **Subfields used:** `a`,`0`,`1`
- **Usage:**
	- **'a'**
		- Alternate primary name
		- Preferred alternate name
	- **'0'** and **'1'**
		- Equivalent relationships to **LCNAF, Wikidata and VIAF**

---

### Field 688
- **Occurrences:** `1,500,540`
- **Mapped:** `N`
- **Notes:**  
  - This field documents the application history of the heading and **does not map to Linked.art**.

---

### Field 400
- **Occurrences:** `983,354`
- **Mapped:** `Y`
- **Subfields used:** `'a', 'q', 'd'`
- **Usage:**
  - **'a'** and **'q'**  
    - Alternate primary name 
    - Preferred alternate name  
  - **'d'**  
    - Alternate birth and death dates

---

### Field 016
- **Occurrences:** `843,383`
- **Mapped:** `N`
- **Notes:**  
  - Control number field, but only repeats **FAST control number** in `001`, which LUX uses for the identifier.

---

### Field 024
- **Occurrences:** `843,383`
- **Mapped:** `N`
- **Notes:**  
  - Identifies **standard identifiers**.  
  - LUX uses control number `001` to build the URI.

---

### Field 040
- **Occurrences:** `843,383`
- **Mapped:** `N`
- **Notes:**  
  - Identifies the **institution** creating, modifying, or maintaining the bibliographic record.  
  - This is evident in **LUX via the URI** and not explicitly mapped.

---

### Field 100
- **Occurrences:** `843,383`
- **Mapped:** `Y`
- **Subfields used:** `'a', 'd'`
- **Usage:**
  - **'a'**  
    - Preferred primary name
  - **'d'**  
    - Alternate birth and death dates

---

### Field 374
- **Occurrences:** `142,255`
- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Name of occupation
  - **'0'**
    - URI of occupation
---
### Field 370
- **Occurrences:** `141,085`
- **Mapped:** `Y`
- **Subfields used:** `'c','e'`
- **Usage:**
  - **'c'**
    - Preferred residence data point
  - **'e'**
    - Alternate residence data point

---
### Field 046
- **Occurrences:** `111,851`
- **Mapped:** `Y`
- **Subfields used:** `'f','g'`
- **Usage:**
  - **'f'**
    - Preferred birth date
  - **'g'**
    - Preferred death date
---
### Field 377
- **Occurrences:** `97,789`
- **Mapped:** `N`
- **Notes:**  
  - Identifies the **language** associated with a person and **does not map to Linked.art.**

---
### Field 682
- **Occurrences:** `83,426`
- **Mapped:** `N`
- **Notes:**  
  - Identifies deleted or replaced headings. The **Loader** for FAST will check this subfield and not load records that are deleted.
---
### Field 373
- **Occurrences:** `82,226`
- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Name of Group membership
  - **'0'**
    - URI for Group membership
---
### Field 375
- **Occurrences:** `81,588`
- **Mapped:** `Y`
- **Subfields used:** `'a','0'`
- **Usage:**
  - **'a'**
    - Name of gender
  - **'0'**
    - URI for gender

---
### Field 372
- **Occurrences:** `71,374`
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
### Field 053
- **Occurrences:** `54,805`
- **Mapped:** `N`
- **Notes:**  
  - Identifies works associated with an entity by LCC number and **does not map to Linked.art**.
---

### Field 378
- **Occurrences:** `38,677`
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

### Field 368
- **Occurrences:** `13,753`
- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Classification of Person
---
### Field 500
- **Occurrences:** `8,160`
- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Biographical note
---
### Field 510
- **Occurrences:** `5,590`
- **Mapped:** `N`
- **Notes:**
  - Identifies a reference to a bibliographic resource that describes the Person and **is not used by LUX.**
---
### Field 072
- **Occurrences:** `4,798`
- **Mapped:** `N`
- **Notes:**
  - Identifies a subject category code from a controlled vocabulary. However, URIs are not given, and the code string does not reconcile  to any of LUX's sources. 

---
### Field 450 and 410
- **Occurrences:** `2,236`
- **Mapped:** `Y`
- **Subfields used:** `'a'`
- **Usage:**
  - **'a'**
    - Alternate primary name
    - Preferred alternate name
---
### Field 371
- **Occurrences:** `1,310`
- **Mapped:** `N`
- **Notes:**
  - Identifies an address associated with the Person. In the existing data, this is largely email addresses (and thus not used by LUX), or residences already found in **370**.
---

## Corporate:
**Maps to:** Linked.art `Group` Class

**Good identifiers for testing:** ``,``,``

---
### Field 710
- **Occurrences:** `801,923`
- **Mapped:** `Y`
- **Subfields used:** `'a', 'b'`
- **Usage:**
  - **'a'**
    - Preferred primary name, part 1
  - **'b'**
    - Preferred primary name, part 2

---





Dates are extracted if possible.

Field 688: 755570 occurrences

Field 410: 643979 occurrences

Field 016: 405827 occurrences

Field 024: 405827 occurrences

Field 040: 405827 occurrences

Field 110: 405827 occurrences

Field 510: 116360 occurrences

Field 370: 35402 occurrences

Field 368: 23270 occurrences

Field 372: 19015 occurrences

Field 682: 18390 occurrences

Field 377: 16348 occurrences

Field 551: 15421 occurrences

Field 550: 11199 occurrences

Field 046: 6161 occurrences

Field 371: 4703 occurrences

Field 500: 2765 occurrences

Field 373: 2409 occurrences

Field 700: 815 occurrences

Field 411: 806 occurrences
