## Project 1 Report


### 1. Basic information
 - Team #: 13
 - Github Repo Link: https://github.com/wjson2002/cs222-winter24-wjson2002
 - Student 1 UCI NetID: wujp1
 - Student 1 Name: Jason Wu



### 2. Internal Record Format

- Show your record format design.


- Describe how you store a null field.



- Describe how you store a VarChar field.



- Describe how your record design satisfies O(1) field access.


### 3. Page Format
- Show your page format design.

![](C:\Winter2024\proj1PageFormat.png)

- Explain your slot directory design if applicable.

Records are tracked using a slot directory (SD) that starts from the end of a page.
Each SD contains the total free size in page (F), number of records (N), and SD entries that hold a offset and length of record.
Records are then inserted into the page to the first empty location.


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

loop(Number of Pages)
- read page into empty buffer
- check if available page size less than record size
- insert into page
- flush to file

if no page found
- add new page and insert to page

###
- How many hidden pages are utilized in your design?

Use 1 hidden page that stores number of pages, read,write,append.

- Show your hidden page(s) format design if applicable
One page of size 4096 is allocated to be the hidden page.
This page stores each variable as a unsigned int at the start of the hidden page.


### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)