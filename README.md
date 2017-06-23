# insight-pipeline

  I consulted with a wedding planning startup at my Insight fellowship to help migrate their data from PostgreSQL to a more suitable database for their data model -- Redshift.  All of their
  web and mobile traffic is tracked by [segment.io](http://segment.com) to a variety of ingestion points including a postgreSQL database.  The database architecture plan I was asked to implement helped execute in-demand queries including cohort analysis and cross wedding patterns in a way where filtering by time is seemless. The postgreSQL database in its given state could adequately address frequencies for single events and some modest joins. 

  In this document, I will demonstrate key aspects of the data engineering infrastructure that helped with their efforts.  Their main event table was of the form shown below:

```
CREATE TABLE events(
	id varchar(254),
	event_type text,
	timestamp timestamp with time zone,
	<other event column> <type of event column>
	...
);
```
The size of the table was hundreds of million of rows long. A suitable solution to easing use of this table might be to put a B+ tree index on the timestamp as is often done in relational databases.  This however does not solve the intrinsic problems associated with disk space traversal on a transactional row-based database.  Furthermore, To facilitate running queries against any given event type, this information was duplicated in separate event tables for each `event_type` along with other information particular to that event of the following form.

```
CREATE TABLE liked_a_song(
	id varchar(254),
	timestamp timestamp with time zone,
	songName TEXT,
	<other event column> <type of event column>
}

CREATE TABLE commented_on_forum(
	id varchar(254),
	timestamp timestamp with time zone,
	comment TEXT,
	<other event column> <type of event column>
	...
}
```
When there are only two event types, it might be irritating to join these event tables together for a query that concerns multiple events and the particular columns in those tables.  When there are hundreds of tables the situation becomes impossible.  As the data scales to larger tables, queries not only become impossible to construct, but in a row-based transactional database, the queries take an impossibly long amount of time.  My solution to the problem was to append the tables in a way that preserved the common columns defined by `events` in the order given by `events` while appending specific columns such as `comment` and `songName` in a JSON field.  This simple rule based string parsing logic can be encapsulated in a map operation.

Because I anticipated that growth in event traffic would need to accomodate real-time feedback in addition to the fact that my join is simply an atomic operation, I decided to use Apache Kafka to ingest the data through PostgreSQL.  Because it is perfectly reasonable to have PostgreSQL output log files that document insertions to its database, I forwarded the PostgreSQL data to Amazon S3.  While data was streamed to the Producer, the data from the Consumer was batched to Redshift using an remote [SSH Copy](http://docs.aws.amazon.com/redshift/latest/dg/loading-data-from-remote-hosts.html) as outlined on Amazon's documentation.

In summary, the pipeline was thus:

![Pipeline](https://github.com/dataDiet/insight-pipeline/images/pipeline.png)

The multiple Kafka consumers help distribute the column joining operations described above and the Kafka architecture as a whole can be used to efficiently and expeditiously send messages to Redshift.  You might ask why the data was not sent to Redshift in individual calls conducted over JDBC.  The reason has to do with the inefficiency of consecutive single row insertions as outlined [here](http://docs.aws.amazon.com/redshift/latest/dg/c_loading-data-best-practices.html).

The schema of the database in Redshift is relatively straightforward.  Each table in Redshift can have a sortkey and distribution key.  In my implementation I chose to make the distribution key the id field for both the events and metadata tables.  The form of my metadata tables was essentially an `id` for every customer in the company, and associated relatively stable information.  The distribution key was distributed with distribution style EVEN for the large events table and the metadata table was distributed with style ALL.  The Amazon documentation provides a tutorial behind these options [here](http://docs.aws.amazon.com/redshift/latest/dg/tutorial-tuning-tables.html).  In short, these choices are optimal for joins between a very small table and a very large table as the most frequent use case.  If the small metadata table is copied over multiple slices it is not a huge penalty because it is a small table.  Because activities concerning the same `id` are ensured to be collocated by the EVEN style on the events table, the join between the two tables will occur in a collocated manner at all times. As for a sort key, the easy and obvious choice is the timestamp column as the length of these tables is most easily reduced by a filter by time.

