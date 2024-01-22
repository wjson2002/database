## Project 1 Report


### 1. Basic information
 - Team #: 13
 - Github Repo Link: https://github.com/wjson2002/cs222-winter24-wjson2002
 - Student 1 UCI NetID: wujp1
 - Student 1 Name: Jason Wu



### 2. Internal Record Format

- Show your record format design.
![img_3.png](img_3.png)

- Describe how you store a null field.
I store the null field at the start of each record. 


- Describe how you store a VarChar field.
I use a fixed length of size attr.length to store the field.


- Describe how your record design satisfies O(1) field access.
Each field has a pointer to it in the slot directory allowing for O(1) access

### 3. Page Format
- Show your page format design.

![](proj1PageFormat.png)

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

Slot Directory needs reserved space so that adding records to the page does not overwrite the directory.
Need to find a balance of free space to space allocated for directory. 




### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.



### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)

Spent a lot of time fixing bugs and unable to pinpoint why write/read records fails in some cases.

- Feedback on the project to help improve the project. (optional)

Project one is somewhat tough, as I am also very rusty in C++. I feel like only implementing Paged Manager is enough for project one.
Record Manager should be pushed to project two.