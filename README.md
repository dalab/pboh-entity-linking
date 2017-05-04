# pboh-entity-linking

PBoH Entity Linking system.

Code: beta version (will be refactored)

Paper: "Probabilistic Bag-Of-Hyperlinks Model for Entity Linking" , Ganea O-E et al. , (proc. WWW 2016), http://dl.acm.org/citation.cfm?id=2882988

Slides: http://people.inf.ethz.ch/ganeao/WWW16_slides_PBOH.pdf

Other results: 
results on the newest version 1.2.4 of Gerbil can be found here: http://gerbil.aksw.org/gerbil/experiment?id=201610270004 . The InKB results are the ones of interest (since we do not address the issue of out of KB entities). Please note that there is a drop in performance of all systems from version 1.1.4 to version 1.2.* of Gerbil (details here: https://github.com/AKSW/gerbil/issues/98)


To run the code, use 'mvn package'. The jars will be found in the target/ folder. However, it requires various indexes files made from Wikipedia, Crosswikis and the test datasets:

UPDATE: indexes are now available online : https://polybox.ethz.ch/index.php/s/IOWjGrU3mjyzDSV . They are required in various places, but mostly in index/AllIndexesBox.scala . The files whose names end in _part* need to be concatenated in one big file without these suffixes before being used.
Please contact us (octavian.ganea at inf dot ethz dot ch) to receive the required password and for other questions you might have.
