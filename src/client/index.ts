import amqp from "amqplib";
import {
	clientWelcome,
	commandStatus,
	getInput,
	printClientHelp,
	printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
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

	const gameState = new GameState(clientName);

	while (true) {
		const words = await getInput();
		if (!words[0]) {
			continue;
		}

		if (words[0] === "spawn") {
			try {
				commandSpawn(gameState, words);
			} catch (err) {
				console.log((err as Error).message);
			}
		} else if (words[0] === "move") {
			try {
				commandMove(gameState, words);
				console.log("Move successful");
			} catch (err) {
				console.log((err as Error).message);
			}
		} else if (words[0] === "status") {
			await commandStatus(gameState);
		} else if (words[0] === "help") {
			printClientHelp();
		} else if (words[0] === "spam") {
			console.log("Spamming not allowed yet!");
		} else if (words[0] === "quit") {
			printQuit();
			break;
		} else {
			console.log(`Unknown command: ${words[0]}`);
		}
	}

	await conn.close();
}

main().catch((err) => {
	console.error("Fatal error:", err);
	process.exit(1);
});
