
import WebSocket from "ws";

const ws = new WebSocket("ws://localhost:3000");

ws.on("open", () => {
  console.log("Connected to Fleet Tracking WebSocket!");
});

ws.on("message", (message) => {
  console.log("LIVE UPDATE:", JSON.parse(message.toString()));
});

ws.on("error", (error) => {
  console.error("WebSocket error:", error.message);
});

