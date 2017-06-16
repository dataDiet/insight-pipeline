# insight-pipeline

  I consulted with a wedding planning startup at my Insight fellowship.  All of their
  web and mobile traffic is tracked by [segment.io](http://segment.com) to a variety of ingestion points including a postgreSQL database.  The database architecture plan I was asked to implement, helped execute JOY's most in-demand queries, particularly time series based queries.  

  In this document, I will demonstrate key aspects of the data engineering infrastructure that helped with their efforts.  Their main event table was of the form shown below:

```
CREATE TABLE events(
	id varchar(254),
	event_type text,
	timestamp timestamp with time zone,
	ip_address text
);
```
The size of the table was on the order of 10^8 rows. To facilitate running queries against any given event type, this information was duplicated in separate event tables for each `event_type`.  In addition, authenticated account creation with the site along with authenticated login events were also tracked in similarly time series oriented tables of the following forms:
```
CREATE TABLE registration(
	id varchar(254),
	wedding_id text,
	name text,
	timestamp timestamp with time zone
);
CREATE TABLE signin(
	id varchar(254),
	wedding_id text,
	timestamp timestamp with time zone
);
```
The value of information contained in `events` is essentially time-series in form.  Each registration would result in strictly one `event_id` in `registration`. Multiple `id` are associated with each `event_id`.  However, it is possible to have multiple `id` `event_id` combinations in `signin`.  A possible augmentation of this setup might instead simply have a single entry in `signin` for the first time a user signs in.  This is because the most likely use case of querying against these tables comes from joining these relatively small essentially metadata tables and joining against the `event` table.

The more serious and intractable issue was the size of the `events` table itself.  The fact that postgreSQL is optimized for transactional row-based processing for mutable data makes working with such a large table that is much longer than it is wide, intractable.  However, the data in `events` is essentially immutable.  Furthermore, the postgreSQL database existed on a single undistributed datawarehouse.  My production prototype then transitioned the database to Amazon Redshift, a columnar, distributed, managed, relational database.  By comparing query execution plans in postgreSQL and Redshift, I will provide an introduction to the inherent difficulties of these two databases.  Where appropriate, I will provide Java implementations of both these platforms to further illustrate why a columnar distributed databases scales well in this analytics use case. 

We will first start with two fairly simple queries.
```
SELECT count(*) FROM events;
SELECT event FROM events WHERE timestamp >= current_timestamp - INTERVAL '30 day';
```
The query plans for these two queries in postgreSQL are shown below.
```
```
