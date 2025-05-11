# Databricks For Big Data Analysis Project

You are a data scientist / AI engineer whose client wishes to gain further insight into
clinical trials. You are tasked with answering these questions, using visualisations where
these would support your conclusions.

You should address the following questions.
1. The number of studies in the dataset. You must ensure that you explicitly check
distinct studies.
2. You should list all the types (as contained in the Type column) of studies in the
dataset along with the frequencies of each type. These should be ordered from
most frequent to least frequent.
3. The top 5 conditions (from Conditions) with their frequencies.
4. Find the 10 most common sponsors that are not pharmaceutical companies, along
with the number of clinical trials they have sponsored. Hint: For a basic
implementation, you can assume that the Parent Company column contains all
possible pharmaceutical companies.
5. Plot number of completed studies for each month in 2023. You need to include your
visualization as well as a table of all the values you have plotted for each month.
You are to implement all 5 tasks 3 times: once in Spark SQL and twice in PySpark (once
in RDD and another time in DataFrame).

For your second task, you are working with a dataset extracted from Steam, an online video
game distribution service. This dataset is available on Blackboard and named steam-
200k.csv. It provides details on the games different members have purchased and played,
along with the number of hours they have played each game. It contains four columns:
➢ The first column contains a unique identifier for each member
➢ The second column contains the name of the game they purchased or played
➢ The third column contains details of the member behaviour, either ‘purchase’ or
‘play’. Because a game has to be purchased before it can be played there will be two
entries for the same game / member combination in some instances
➢ The fourth is set to 1 for rows where the behaviour is ‘purchase’. For rows where the
behavious is ‘play’ the value in the fourth column corresponds to the number of
hours of play
We can use both purchase and play behaviours as implicit user feedback, which is useful for
training a recommender system.
Your task as a data scientist is to do the following:
➢ Load the dataset into a Spark DataFrame. You may want to consider carrying out
some initial exploratory analysis of the data, which you are welcome to do using
DataFrames, Spark SQL, Databricks visualisations, another visualisation library etc.
➢ Use MLlib to train a collaborative filtering recommender system on the provided
data, evaluate its performance and explore some of the resulting
recommendations. You will need to carry out all pre-processing steps, such as
splitting the data into training and test sets. It is your decision whether to include
both ‘purchase’ and ‘play’ behaviours or to choose one of these as more suitable
for your purposes. You may wish to experiment with more than one approach

The data necessary for these tasks will be zipped CSV files. The .csv files have a header
describing the files’ contents. They are:
1. Clinicaltrial_2023.csv:
Every row in the dataset corresponds to an individual clinical trial and is identified
by different variables. It's important to note that the first column contains a mixture
of various variables separated by a delimiter, and the date columns exhibit various
formats. Please consider these issues and ensure that the dataset is appropriately
prepared before initiating any analysis.
(Source: ClinicalTrials.gov)
2. pharma.csv:
The file contains a small number of a publicly available list of pharmaceutical
violations. For the purposes of this work, we are interested in the second column,
Parent Company, which contains the name of the pharmaceutical company in
question.
(Source: https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals)
