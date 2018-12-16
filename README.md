# Title
Study of the fidelity of media, as representations of the society state

# Abstract
We all know that the media have a strong influence on our information-driven societies. We could imagine that their primary purpose would be to inform objectively and to reflect the state of mind of all, or part of, the population. But are they really ? We also hear a lot about the polarization of the media, but what is it really about?  It is always useful to know under what degree of objectivity the information is transmitted, to avoid being influenced too easily.

In our project we aim to analyze the relation between the media and the current state of our society. In order to do so, we will use the GDELT V2.0 dataset, allowing to explore on several axes, such as time, localisation and recurring themes. Moreover, based on the UN “World happiness report”, we will look if there is a correlation between the state of mind of the inhabitants, as well as, some important political factors such as corruption, confidence in the government and democracy index per country and their respective media landscape. 

# Research questions 
We will guide our work with the following questions: 
  - Are media anxiogenic in their way of telling stories? 
  - Is there a correlation between a country and the emotions transmitted by its media ? 
  - Can we find a link with its political system ? 
  - Can media reliably translate the state of happiness of a country ?  
  - Protests, events which have a considerable amount on the stability of countries, are remarkably over-mentioned in the media for specific countries?
  - Can protests be related to the country intrinsic factors such as population, political regime?  

# Dataset
[GDELT V2](https://www.gdeltproject.org/data.html#documentation) gathering a huge amount of information about events happening all around the world and their corresponding media coverage. Especially, thanks to the [Global Content Analysis Measures (GCAM)](http://blog.gdeltproject.org/introducing-the-global-content-analysis-measures-gcam/), it extracts the sentiments expressed in the different information sources. This dataset is composed of three distinct parts : Event table, Mention table and a Global Knowledge Graph (GKG).
  * **Event table** uniquely records global events, along with principal components and actors. We will be mostly interested in the fields GlobalEventId, EventRootCode, GoldsteinScale, AvgTone and ActionGeo_CountryCode .
* **Mention table** records all individual mentions of each event. We will be interested in similar fields as for the Event table.
In these two tables, records are stored one per line, tab-delimited.
* **GKG** records a list of articles and documents along with their main themes and associated events. We will mainly focus on fields GKGRecordId, Date, Locations, Tone and GCAM. More details about this columns can be found on the [codebook](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.pdf)  related to GKG dataset.
The file format of the GKG is more sophisticated and will require more preprocessing. However, it has also a ‘.csv’ extension and it is tab-delimited.

For the purposes of this work, GKG has been omitted.

More details about this columns can be found on the [codebook](http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.pdf)  related to GKG dataset.
In order to use this rather big dataset, we will have to preprocess it on Google BigQuery, a data processing interface which contains already the GDELTV2 data and an extremely helpful tool enabling processing tha large data very fast.

  - [World Happiness Report](http://worldhappiness.report/) gathers data objective data on the state of mind of the inhabitants of most countries of the world. This rather simple CSV dataset also contains other interesting state indicators such as the  *Corruption*, *Confidence in the government*.

  - [Democracy Index 2017](https://en.wikipedia.org/wiki/Democracy_Index), index compiled by the Economist Intelligence Unit (EIU), gives the ranking of 167 countries regarding to their democracy quality. 

  - A gentle reminder: We decided to use the Democracy Index report to have an idea about country democracy qualities rather than the World Happiness Report which also contains information about democracy of countries for the one and only reason which is Democracy Index report allows us to put countries in 4 different categories namely *full democracy*, *flawed democracy*, *hybrid regime* and *authoritarian*.


# Highlights:
  * The following graph shows that media are following a negative tone whatever the event is:
  ![Average Tone](AvgTone.png)

  * The following figure states that there is a positive correlation between corruption and the weight of protest news among all the news for a     country and a negative correlation between happiness score, confidence in government, democracy index and again the importance of protest news in media for a country:

  <p align="center">
    <img width="360" height="300" src="protest-indicators.png">
  </p>

  * One can visualize the box plots for correlation factors between protest events and democracy quality and population for each category of democracy (Authoritarian, Hybrid Regime, Flawed Democracy, Full Democracy) on the following figure:
  <p align="center">
    <img width="460" height="460" src="boxplots.png">
  </p>





# Work Repartition
  * Sinan Gökçe:

  * Juliane Dervaux:

  * Lucas Burget:  





