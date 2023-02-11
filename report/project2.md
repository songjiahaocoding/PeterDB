## Project 2 Report


### 1. Basic information
 - Team #:
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-songjiahaocoding
 - Student 1 UCI NetID: jiahaos7
 - Student 1 Name: Jiahao Song

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
    - Tables:
      - table-id: int
      - table-name: varchar(50) The name of the table
      - file-name: varchar(50) The name of the file which stores the data of the table
    - Columns:
      - table-id: int
      - column-name: varchar(50) The name of the attribute
      - column-type: int The type of the attribute which is associated with AttrType
      - column-length: int The length for the attribute for attributes which is type of varchar
      - column-position: int Used to identify the position of the attribute in the record
    - Variables:
      - No columns. Only one record to store the number of tables.


### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

   - I added the original RID at the end of each record. For record that is moved to other places, it's the RID of previous record.
   - The other design is the same.


- Describe how you store a null field.
    - Same as P1


- Describe how you store a VarChar field.
    - Same as P1


- Describe how your record design satisfies O(1) field access.
    - Same as P1


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.
    - Same as P1


- Explain your slot directory design if applicable.
    - Same as P1


### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?
    - Same as P1


- Show your hidden page(s) format design if applicable
    - Same as P1


### 6. Describe the following operation logic.
- Delete a record
  - First check if the record is Tombstone or not.
    - If it's a Tombstone, then perform a recursive deleteRecord first
  - Delete the current record by setting the data to 0
  - Moving all the records behind it to the left to fill the gap
  - Update the information and write back to the file


- Update a record
    - If it's a Tombstone, perform a recursive updateReocrd
    - If the new record can fit into the old one, update the information and compact the page
    - If the new record is the last one and there is enough space for it, update the information and compact the page
    - If the new record is too big
      - Find a page that has enough space for this record
      - Reuse deleted slot in that page if applicable
      - Write the new record on that page and leave a Tombstone in the previous position which points to the new location
      - Update information and compact the page


- Scan on normal records
    - Retrieve the attribute which is used as the query condition. 
    - Make comparison between retrieved attribute and the condition.
    - Build the returned data using the selected attributes.


- Scan on deleted records
    - Identify the deleted record using the slot information.
    - If there is a DELETE_MARK, it means the record associated with this slot is deleted
    - Then just jump to the next slot, perform the scan operation.


- Scan on updated records
    - In my design, I will pass all tombstones and retrieve the real record when the scan reaches the position.
    - So, when I retrieved the record I will decide whether this record is Tombstone or not. 
      - If it's tombstone, I will just ignore it and continue the scan.
      - If it's not, I will just perform the normal operation.


### 7. Implementation Detail
- Other implementation details goes here.



### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)