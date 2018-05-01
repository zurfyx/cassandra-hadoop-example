# Cassandra Hadoop Example

A modification of the [Apache WordCount](http://grepcode.com/file/repo1.maven.org/maven2/org.apache.hadoop/hadoop-mapreduce-examples/2.6.0/org/apache/hadoop/examples/WordCount.java) to count different browser agents on a live Cassandra cluster.

## Getting started

**Your own IP address**

Edit `WordCountCassandra` variables for your own Cassandra variables. Keep in mind that Cassandra requires Thrift port (9160) open.

**Build .jar**

```
mvn package
```

**Copy to your own server**

Copy `target/` artifact into your server.

## Database setup

```
create keyspace nyao with replication = { 'class': 'SimpleStrategy', 'replication_factor': 3 };

create table visitors (
	bucket date,
	agent text,
	ip text,
	created_at timestamp,
	primary key (bucket, created_at)
) with clustering order by (created_at desc);

CREATE TABLE count (
  name text PRIMARY KEY,
  msg int
);
```

## [Optional] Node.js Web Server

```
#!/usr/bin/env node

const http = require('http');
const cassandra = require('cassandra-driver');

const LOAD_BALANCER = '::ffff:10.131.68.5';
const CASSANDRA_NODES = ['10.131.68.15'];
const HTML_HEAD = 'nyao-web-1 @ 206.189.31.189';

const client = new cassandra.Client({ contactPoints: CASSANDRA_NODES, keyspace: 'nyao' }); 

const reqHandler = async (req, resp) => {
	const ip = req.connection.remoteAddress === LOAD_BALANCER ? req.headers['x-forwarded-for'] : req.connection.remoteAddress;
	const agent = req.headers['user-agent'];
	const now = new Date();

	// Save new entry.
	const insertQuery = 'INSERT INTO visitors(bucket, agent, ip, created_at) VALUES (?, ?, ?, ?)';
	const paramsQuery = [now, agent, ip, now];
	await client.execute(insertQuery, paramsQuery, { prepare: true });

	// Get latest 100
	const selectQuery = 'SELECT agent, ip, created_at FROM visitors WHERE bucket=? LIMIT 100';
	const paramsQuery2 = [now];
	const entries = await client.execute(selectQuery, paramsQuery2, { prepare: true });

	const html = `
	<html>
	<head>
		<title>nyao.io</title>
	</head>
	<body>
		<h1>${HTML_HEAD}</h1>
		<ul>
		${entries.rows.map((row) => (`
			<li>
				<div>${row['created_at']}</div>
				<div>${row.agent}</div>
				<div>${row.ip}</div>
			</li>
		`))}
		</ul>
	</body>
	</html>
	`;
	resp.end(html);
};

http.createServer(reqHandler).listen(80);
```

## Resources

- https://www.slideshare.net/rastrick/presentation-12982302 
- https://cdn.oreillystatic.com/en/assets/1/event/100/An%20Introduction%20to%20Real-Time%20Analytics%20with%20Cassandra%20and%20Hadoop%20Presentation.pdf
- https://web.archive.org/web/20160405165613/http://gerrymcnicol.com:80/index.php/2014/01/21/hadoop-and-cassandra-part-7-modifying-your-mapreduce-code-to-read-and-write-from-cassandra/ 
- https://www.digitalocean.com/community/tutorials/how-to-install-hadoop-in-stand-alone-mode-on-ubuntu-16-04
- https://www.digitalocean.com/community/tutorials/how-to-install-cassandra-and-run-a-single-node-cluster-on-ubuntu-14-04
- https://www.digitalocean.com/community/tutorials/how-to-run-a-multi-node-cluster-database-with-cassandra-on-ubuntu-14-04  
- https://blog.cloudera.com/blog/2014/06/how-to-create-an-intellij-idea-project-for-apache-hadoop/ 
- https://stackoverflow.com/questions/36133127/how-to-configure-cassandra-for-remote-connection   
- https://github.com/jpgreenwald/hadoop-cassandra-example

## License

MIT © [Gerard Rovira Sánchez](//zurfyx.com)
