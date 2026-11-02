import amqp from "amqplib";

async function main() {
	console.log("Starting Peril server...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const conn = await amqp.connect(connectionString);
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
