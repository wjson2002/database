## Debugger and Valgrind Report

### 1. Basic information
 - Team #: 13
 - Github Repo Link: 
 - Student 1 UCI NetID: wujp1
 - Student 1 Name: Jason Wu
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Using a Debugger
- Describe how you use a debugger (gdb, or lldb, or CLion debugger) to debug your code and show screenshots. 
For example, using breakpoints, step in/step out/step over, evaluate expressions, etc. 
![img.png](img.png)
Used Clion debugger to check the values of variables such as RID.
### 3. Using Valgrind
- Describe how you use Valgrind to detect memory leaks and other problems in your code and show screenshot of the Valgrind report.
![img_1.png](img_1.png)
I use valgrind to trace errors that are shown.
![img_2.png](img_2.png)
This image uses that I have a potential read error at line 228. Based on these Valgrind messages I can more efficient find errors.
- 