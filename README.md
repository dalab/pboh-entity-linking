# pboh-entity-linking

PBoH Entity Linking system.

Code: beta version 

Paper: "Probabilistic Bag-Of-Hyperlinks Model for Entity Linking" , Ganea O-E et al. , (proc. WWW 2016), http://dl.acm.org/citation.cfm?id=2882988

Slides, poster, online system and comparison with existing systems : http://people.inf.ethz.ch/ganeao

Newest GERBIL results: 
- PBOH is now fully integrated with GERBIL (D2KB setting). It can be used as a webservice: http://gerbil.aksw.org/gerbil/config 
- results on the newest version 1.2.4 of Gerbil can be found here: http://gerbil.aksw.org/gerbil/experiment?id=201610270004 . The InKB results are the ones of interest (since we do not address the issue of out of KB entities). Please note that there is a drop in performance of all systems from version 1.1.4 to version 1.2.* of Gerbil (details here: https://github.com/AKSW/gerbil/issues/98). 


Indexes download link: https://polybox.ethz.ch/index.php/s/IOWjGrU3mjyzDSV . They are required in various places (i.e. wherever there are file paths containing the prefix '/media/hofmann-scratch/'). The files whose names end in _part* need to be concatenated in one big file without these suffixes before being used, e.g. one file called anchorsListFromEachWikiPage.txt_dev_index will be made by merging all files anchorsListFromEachWikiPage.txt_dev_index.part_*. The provided indexes are already in the suitable format for indexes that are loaded in here: https://github.com/dalab/pboh-entity-linking/tree/master/src/main/scala/index . For the eval datasets , there are indications where to get the data from at the beginning of each file in here: https://github.com/dalab/pboh-entity-linking/tree/master/src/main/scala/eval/datasets For the AIDA dataset, a sample of the format is shown here: https://github.com/dalab/pboh-entity-linking/issues/3
Please contact octavian.ganea at inf dot ethz dot ch to receive the required password and for other questions you might have.

How to run the code:
- Download the above indexes and update their locations inside the code. Do the same for the test sets.
- Compile with 'mvn package'. Will generate a self-contained jar called target/PBoH-1.0-SNAPSHOT-jar-with-dependencies.jar
- Run the code to test PBOH on the datasets mentioned in the paper using the command: scala -J-Xmx90g target/PBoH-1.0-SNAPSHOT-jar-with-dependencies.jar testPBOHOnAllDatasets max-product
- A new dataset can be added as follows: one needs to write a class similar to eval/datasets/AQUAINT_MSNBC_ACE04.scala that transforms an input text file with entity annotations into an object of type Array[(String, Array[(String,Int, Array[Int])])]. Each element of this list is a pair (doc_name, doc_annotations) in which doc_annotations is a list of entity annotations from the given document in the format  ((mention.toLowerCase(), entity, context)). Context is here a list of word IDs of words surrounding the given mention in a window of fixed size. The word IDs are obtained from word strings using the in-memory dictionary index/WordFreqDict.scala.   
