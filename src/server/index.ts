import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
	console.log("Starting Peril server...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const conn = await amqp.connect(connectionString);
	const confChannel = await conn.createConfirmChannel();
	await publishJSON(confChannel, ExchangePerilDirect, PauseKey, {
		isPaused: true,
	});
	console.log("Connection successful");
	process.on("SIGINT", async () => {
		console.log("Attempting shutdown");
		await conn.close();
		console.log("Shutdown complete");
		process.exit(0);
	});
}

main().catch((err) => {
	console.error("Fatal error:", err);
	process.exit(1);
});
