# Task 1
## Session
The session entity was implemented as stipulated in the project documentation.

```sessionType``` implemented as a ```StringProperty``` with choices to establish consistency across the app.

The following was added to achieve function goals:

- ```speakerKey```: A web safe key for the speaker of the session. See **Speaker** below
- ```conferenceKey```: a web safe key of the conference this session is part of.

Multiple sessions can be added for a conference.


## Speaker
In a multi user environment where certain properties are open to various forms of spelling and typing errors, its best to make sure the data is kept consistent, especially where person names are involved.

A speaker entity was created to solve most of the consistency worries. Users are free to re-use already existing speaker names, or where the speaker is not yet available in the data store, add the data themselves.

Some other advantages gained:

- The look and feel of the app is enhanced because the name is shown consistently across all areas of the app.
- In code, speakers are easier to search and identify because unique keys exists

# Task 3
The problem is that an inequality filter can only be applied to one property in a query. In this case, two properties need inequality filters, making it impossible to create a single query.

To work around this problem, I would create two keys-only queries, one for non-workshop sessions, and another for sessions before 7 PM. I would then use Pyhton's set functionality to find the common elements in both queries, which should give us a set of the session entity keys satisfying both criteria. We can then use the set of keys to retrieve the sessions entities for further processing.

## Item 2

### getConferenceSpeakers
Return all the speakers at a conference. Great overview tool for the user.
 
### getMarketableConferences 
For marketing purposes, find the conferences that is in progress and not yet fully booked. This is great for promoting empty seats at discounted prices.

The following message forms were created:

- MarketableConferenceForm
- MarketableConferenceForms