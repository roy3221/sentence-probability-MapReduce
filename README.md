# sentence-probability-MapReduce

##Description
Build a program to calculate the probability of sentences from a  sample of corpus. From the corpus sample, you can get the probability of a word that exists at  position i by using the formula below: 
P(i, w) = Num(i, w) / N; 

where Num(i , w) is the number of times that w exists at i?th position of a sentence, N is the  total number of sentences with at least i words. 

The probability of a sentence is the product of probability of all its individual words.

+ 1  2  3           4             5     6       7    [positions]                   

+  It  is the bueaty of sf life  [  words  ]                        

+ P(s) = P(1 , 'It')P(2, 'is')P(3, 'the')P(4, 'bueaty')P(5, 'of')P(6, 'sf')P(7, life). 

Implement a MapReduce program that  
1. accepts two parameters to execute. One is for file path for the corpus, the other is where  to place your result; 
2. generates the top three sentences with the largest existence probability in the corpus. 
 
A small sample file for your testing in your standalone environment,  
https://www.dropbox.com/s/ubrivo8sh6x1z1h/Corpus.txt?dl=0 

Implement the program in a distributed cluster.  

#### Amazon EMR Test 
• Compile and package the source code to code.jar 

• Create a S3 bucket 

• Upload code.jar and input file into S3 bucket 

• Start a EMR cluster to execute the CorpusCaculator program as a job. 
