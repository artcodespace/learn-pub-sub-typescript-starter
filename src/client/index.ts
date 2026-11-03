import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
	console.log("Starting Peril client...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const clientName = await clientWelcome();
	const conn = await amqp.connect(connectionString);

	await declareAndBind(
		conn,
		ExchangePerilDirect,
		`${PauseKey}.${clientName}`,
		PauseKey,
		SimpleQueueType.Transient,
	);
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
