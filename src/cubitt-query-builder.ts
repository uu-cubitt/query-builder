declare let require: any;
declare let process: any;
import {CQRSGraph} from "cubitt-graph-cqrs";

let eb = vertx.eventBus();

// Initialize empty graph for now
let projectId = vertx.getOrCreateContext().config().id;

import JDBCClient = require("vertx-jdbc-js/jdbc_client");

let postgresUser = process.env.POSTGRES_USER || "postgres";
let postgresPass = process.env.POSTGRES_PASSWORD || "";
let postgresDb = process.env.POSTGRES_DB || postgresUser;
let postgresHost = process.env.POSTGRES_HOST || "localhost";
let postgresPort = process.env.POSTGRES_PORT || 5432;

let client = JDBCClient.createShared(vertx, {
	"url" : "jdbc:postgresql://" + postgresHost + ":" + postgresPort + "/" + postgresDb + "?user=" + postgresUser + "&password=" + postgresPass,
	"driver_class" : "org.postgresql.Driver",
	"max_pool_size" : 30
});

// Connect to database
client.getConnection(function (conn, conn_err) {
	if (conn_err !== null) {
		console.log("Could not connect to database, shutting service down");
		vertx.close();
		return;
	}
	let connection = conn;

	eb.consumer("projects.query." + projectId, function (message) {
		let graph = new CQRSGraph();
		let version: number | string;
		if (message.body() === "latest"){
			version = "latest";
		} else {
			version = parseInt(message.body());
		}

		if (version === "latest") {
			connection.query("SELECT event FROM \"" + projectId + "_events\"", function(res, res_err) {
				if (res_err) {
					console.log("Could not load events from eventstore " + res_err);
					vertx.close();
					return;
				}
				let results = res.results;
				Array.prototype.forEach.call(results, function(row) {
					let event = row[0];
					graph.ApplyEvent(JSON.parse(event));
				});
				message.reply(JSON.stringify(graph.GetGraph().serialize()));
			});
		} else {
			connection.query("SELECT event FROM \"" + projectId + "_events\" WHERE id <= " + version, function(res, res_err) {
				if (res_err) {
					console.log("Could not load events from eventstore " + res_err);
					vertx.close();
					return;
				}
				let results = res.results;
				Array.prototype.forEach.call(results, function(row) {
					let event = row[0];
					graph.ApplyEvent(JSON.parse(event));
				});
				message.reply(JSON.stringify(graph.GetGraph().serialize()));
			});
		}
	});
});
