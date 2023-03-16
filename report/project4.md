## Project 4 Report


### 1. Basic information
- Team #:
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-songjiahaocoding
- Student 1 UCI NetID: jiahaos7
- Student 1 Name: Jiahao Song


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).
  - There will be an index table associated with each real table
  - The index table will keep track of all the current indexes for now
  - The row index contains the following attributes:
    - table-id
    - table-name
    - attr-name: The name of the attribute which has the index on.


### 3. Filter
- Describe how your filter works (especially, how you check the condition.)
  - I will retrieve the next tuple first and check if it matches the condition
  - If not, keep retrieving until the above satisfies or there is no more next tuple


### 4. Project
- Describe how your project works.
  - Retrieve the next tuple first
  - Build the returned data according to the project attributes and null indicator
  - Return built data


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)
  - Use `map<Key, vector<Record>>` as a buffer
  - Fill the map under the restriction
  - Scan the right table
  - If the key in the next tuple of the right table is not in the map
    - Clear the map as the new tuple is out of range
  - Use a `vector<pair<Record, Record>>` as a buffer for tuples waiting for merging
    - Each element in the vector is the raw data for a merged tuple


### 6. Index Nested Loop Join
- Describe how your index nested loop join works.
  - First retrieve a tuple from left table
  - Use the key in the retrieved tuple as the key to scan the right table
  - Merge the tuple if there is a match
  - Iterate until there is no next tuple in the left table


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.
  - For COUNT, I add 1 to the count variable whenever I get a next tuple. Return this value as the final result.  
  - For MIN and MAX, I keep a variable which represents the max and min value. During the scan, I will update the value.
  - For SUM, I will use a variable to sum up all the value during the scan
  - For AVG, I will behave like SUM. Then use the sum variable and the count variable to compute the AVG 

- Describe how your group-based aggregation works. (If you have implemented this feature)



### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.

  No.

- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)