# Estimating Twitter User Location Using Social Interactions - A Content Based Approach
----------

This is the code used for estimating location of Twitter users using only tweet content. 

The work was published at
[SocialCom 2011] (http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=6113226&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D6113226)

## Environment
==========
1. Ubuntu v12.04+
2. Java SDK v1.7+
3. MySQL (root : admin)
4. Apache Lucene 3.1.0

## Execution
==========
1. Initialize Database and dictionary file containing list of words.
2. Requires "stopwords.txt", "training_set_tweets.txt" & test_set_tweets.txt files.
3. Run `getCityPosition.java`
4. Run `locationMiningTrain.java`
5. Run `locationMiningTest.java`
6. Run `checkAccuracy.java`
7. Run `checkAccuracyModel.java`
8. Check accuracy by executing `topAccuracy.java`.

-----------

