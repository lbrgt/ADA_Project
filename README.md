# Title
Evolution of emotions conveyed by media and their link with the society state

# Abstract
We all know that the media have a strong influence on our information-driven societies. We could imagine that their primary purpose would be to inform objectively and to reflect the state of mind of all, or part of, the population. But are they really ? We also hear a lot about the polarization of the media, but what is it really about?  It is always useful to know under what degree of objectivity the information is transmitted, to avoid being influenced too easily.

In our project we aim to analyze the tone and the sentiments conveyed by them.  In order to do so, we will use the GDELT V2.0 dataset, allowing to explore on several axes, such as time, localisation and recurring themes. Moreover, based on the UN “World happiness report”, we will look if there is a correlation between the state of mind of the inhabitants per country and their respective media landscape. 

# Research questions 
We will guide our work with the following questions: 
  - Are the media becoming more anxiogenic in their way of telling stories? 
  - Is there a correlation between a country and the emotions transmitted by its media ? Can   we find a link with its political system ? 
  - Can we observe an interesting evolution over time (long term or political schedules) ? Are media contributing to the installation of a negative climate before an election ?
  - Can media reliably translate the state of happiness of a country ?  

# Dataset
  * **Event table** uniquely records global events, along with principal components and actors. We will be mostly interested in the fields GlobalEventId, EventCode, Day and AvgTone.
* **Mention table** records all individual mentions of each event. We will be interested in similar fields as for the Event table.
In these two tables, records are stored one per line, tab-delimited.
* **GKG** records a list of articles and documents along with their main themes and associated events. We will mainly focus on fields GKGRecordId, Date, Locations, Tone and GCAM. More details about this columns can be found on the [codebook](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.pdf)  related to GKG dataset.
The file format of the GKG is more sophisticated and will require more preprocessing. However, it has also a ‘.csv’ extension and it is tab-delimited.

More details about this columns can be found on the [codebook](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.pdf)  related to GKG dataset.
In order to use this rather big dataset (112 GB), we will have to connect to the cluster, extract subsets of it in order to build our script and finally launch it on the cluster once it’s done.

  - [World Happiness Report](http://worldhappiness.report/) gather data objective data on  the state of mind of the inhabitants of most countries of the world. This rather simple CSV dataset also contains other interesting state indicators such as the *Democratic quality*.

# A list of internal milestones up until project milestone 2

  - 7/11: Understand how to use cluster ! Find precisely which fields of the different datasets we want to focus on. Explore the very rich possibilities offered by the GCAM content. Deepen our understanding of the complex links between pieces of information.
  - 14/11: Data wrangling and cleaning. Sorting out relevant data for our analysis. 
  - 25/11: Extract interesting information from our gathered data in order to provide answers to the questions listed above.

# Questions for TAa

  - Could you confirm that this project is conceivable and that it will not require the use of Google BigQuery tool ?
  - What is the best approach towards performing tests on the datasets in the cluster? 

