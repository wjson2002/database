## Project 4 Report


### 1. Basic information
- Team #: 13
- Github Repo Link: https://github.com/wjson2002/cs222-winter24-wjson2002/blob/main/report/project4.md
- Student 1 UCI NetID: wujp1
- Student 1 Name: Jason Wu


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).
Did not implement this part.


### 3. Filter
- Describe how your filter works (especially, how you check the condition.)
Filter uses a single for loop to iterate through get tuple provided by the iterator. Filter
will either return EQ_EOF or keep calling getNextTuple until a matching tuple is found.
For my condition, it checks based on the datatype and compares the tuple to the provide
condition during the creation on Filter. Null terminators are ignored.


### 4. Project
- Describe how your project works.
Project also uses a single for loop to iterate through each tuple provided by the iterator.
Then for each tuple it will condense the tuple down to only the attribute that is wanted.
For example if the original tuple contains: (Name, Id, Grade, Score), and project only wants ID and Grade
I will iterate through the wanted attributes (ID and Grade) and retrieve/condense them into their own tuple and return.


### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)



### 6. Index Nested Loop Join
- Describe how your index nested loop join works.
Not implemented.


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).
Grace hash join first creates N number of partitions each is identified by 1-N.
Then I will partition each tuple, starting with the left iterator. The data is sorted into 
the file based on a hash of data % N. Same is done with right iterator.

Then I for each tuple for each left partition I will match it to each tuple in a corresponding right partition


### 8. Aggregation
- Describe how your basic aggregation works.
Aggregation keeps track of each value (min, max, count, sum). If it is the first tuple, min and max will be set to this value
and value is added to sum and count is incremented by 1. For every tuple after it is compared to the current min/max and is updated accordingly.
Count and sum is also updated. Average is calcualted at the end by sum/count.
Once all the tuples are iterated, the data that is wanted (min, max, count, sum, avg) will be returned to user.

Aggregation will only return once.

- Describe how your group-based aggregation works. (If you have implemented this feature)
Not implemented.


### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.
  
No changes.


- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.
Solo work.


### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)

Did not complete project 3, So did not do index related work or pass test cases that used INDEX.


- Feedback on the project to help improve the project. (optional)
