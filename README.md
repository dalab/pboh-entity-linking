# pboh-entity-linking

PBoH Entity Linking system.

Code: beta version 

Paper: "Probabilistic Bag-Of-Hyperlinks Model for Entity Linking" , Ganea O-E et al. , (proc. WWW 2016), http://dl.acm.org/citation.cfm?id=2882988

Slides: http://people.inf.ethz.ch/ganeao/WWW16_slides_PBOH.pdf

Newest GERBIL results: 
- PBOH is not fully integrated with GERBIL (D2KB setting). 
- results on the newest version 1.2.4 of Gerbil can be found here: http://gerbil.aksw.org/gerbil/experiment?id=201610270004 . The InKB results are the ones of interest (since we do not address the issue of out of KB entities). Please note that there is a drop in performance of all systems from version 1.1.4 to version 1.2.* of Gerbil (details here: https://github.com/AKSW/gerbil/issues/98). 


Indexes download link: https://polybox.ethz.ch/index.php/s/IOWjGrU3mjyzDSV . They are required in various places (i.e. wherever there are file paths containing the prefix '/media/hofmann-scratch/'). The files whose names end in _part* need to be concatenated in one big file without these suffixes before being used.
Please contact octavian.ganea at inf dot ethz dot ch to receive the required password and for other questions you might have.

How to run the code:
- Download the above indexes and update their locations inside the code. Do the same for the test sets.
- Compile with 'mvn package'. Will generate a self-contained jar called target/PBoH-1.0-SNAPSHOT-jar-with-dependencies.jar
- Run the code to test PBOH on the datasets mention in the paper using the command: scala -J-Xmx90g target/PBoH-1.0-SNAPSHOT-jar-with-dependencies.jar testPBOHOnAllDatasets max-product
