
import express from "express";
import cors from "cors";
import { createClient } from "redis";
import dotenv from "dotenv";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const httpServer = createServer(app);

const redis = createClient({
  url: process.env.REDIS_URL || "redis://localhost:6379",
});

const redisSubscriber = redis.duplicate();

redis.on("error", (err) => console.error("Redis error:", err));
redisSubscriber.on("error", (err) =>
  console.error("Redis subscriber error:", err)
);

const wss = new WebSocketServer({ server: httpServer });

async function getTruckData(key: string) {
  const data = await redis.hGetAll(key);

  const parseJson = (value?: string) => {
    if (!value) return null;
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  };

  return {
    truckId: key.replace("truck:", ""),
    gps: parseJson(data.gps),
    obd: parseJson(data.obd),
    gpsTimestamp: data.gps_timestamp || null,
    obdTimestamp: data.obd_timestamp || null,
  };
}

async function startServer() {
  await redis.connect();
  await redisSubscriber.connect();

  console.log("Connected to Redis");

  // Paginated fleet listing using Redis SCAN.
  app.get("/api/trucks", async (req, res) => {
    try {
      const cursor = String(req.query.cursor ?? "0");
      const requestedCount = Number(req.query.limit ?? 100);

      if (!/^\d+$/.test(cursor)) {
        return res.status(400).json({
          message: "Invalid cursor",
        });
      }

      if (
        !Number.isInteger(requestedCount) ||
        requestedCount < 1 ||
        requestedCount > 500
      ) {
        return res.status(400).json({
          message: "Limit must be between 1 and 500",
        });
      }

        let scanCursor = cursor;
        let result;

        do {
        result = await redis.scan(scanCursor, {
            MATCH: "truck:*",
            COUNT: requestedCount,
        });

        scanCursor = result.cursor;
        } while (result.keys.length === 0 && scanCursor !== "0");

      const trucks = await Promise.all(
        result.keys.map((key) => getTruckData(key))
      );

      res.json({
        trucks,
        nextCursor: scanCursor,
        hasMore: scanCursor !== "0",
        });
    } catch (error) {
      console.error("Fleet API error:", error);
      res.status(500).json({
        message: "Internal server error",
      });
    }
  });

  // Individual truck endpoint.
  app.get("/api/trucks/:truckId", async (req, res) => {
    try {
      const truckId = req.params.truckId;
      const data = await redis.hGetAll(`truck:${truckId}`);

      if (Object.keys(data).length === 0) {
        return res.status(404).json({
          message: "Truck data not found",
        });
      }

      const parseJson = (value?: string) => {
        if (!value) return null;
        try {
          return JSON.parse(value);
        } catch {
          return null;
        }
      };

      res.json({
        truckId,
        gps: parseJson(data.gps),
        obd: parseJson(data.obd),
        gpsTimestamp: data.gps_timestamp || null,
        obdTimestamp: data.obd_timestamp || null,
      });
    } catch (error) {
      console.error("API error:", error);
      res.status(500).json({
        message: "Internal server error",
      });
    }
  });

  // Redis Pub/Sub -> WebSocket clients.
  await redisSubscriber.subscribe("truck-updates", (message) => {
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  });

  httpServer.listen(3000, () => {
    console.log("API running at http://localhost:3000");
    console.log("WebSocket server running on port 3000");
  });
}

startServer().catch(console.error);
