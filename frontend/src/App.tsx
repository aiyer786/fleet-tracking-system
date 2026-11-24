import L from "leaflet";
import { Fragment, useEffect, useRef, useState } from "react";
import {
  MapContainer,
  TileLayer,
  Marker,
  Popup,
  useMap,
} from "react-leaflet";
import "leaflet/dist/leaflet.css";
import "./App.css";

interface Truck {
  truckId: string;
  gps: {
    latitude: number;
    longitude: number;
    speed: number;
  } | null;
  obd: {
    engineRPM: number;
    fuelLevel: number;
    engineTemperature: number;
    batteryVoltage: number;
  } | null;
  gpsTimestamp?: number | string | null;
  obdTimestamp?: number | string | null;
  lastSeen?: number;
}
const normalTruckIcon = new L.DivIcon({
  className: "truck-marker",
  html: '<div class="marker-dot"></div>',
  iconSize: [18, 18],
  iconAnchor: [9, 9],
});

const selectedTruckIcon = new L.DivIcon({
  className: "truck-marker",
  html: '<div class="marker-dot selected-marker-dot"></div>',
  iconSize: [30, 30],
  iconAnchor: [15, 15],
});
function mergeTruckUpdate(
  previous: Record<string, Truck>,
  incoming: Truck
): Record<string, Truck> {
  const existing = previous[incoming.truckId];

  if (!existing) {
    return {
      ...previous,
      [incoming.truckId]: incoming,
    };
  }

  const gpsIsNewer =
    incoming.gpsTimestamp != null &&
    (existing.gpsTimestamp == null ||
      incoming.gpsTimestamp > existing.gpsTimestamp);

  const obdIsNewer =
    incoming.obdTimestamp != null &&
    (existing.obdTimestamp == null ||
      incoming.obdTimestamp > existing.obdTimestamp);

  return {
    ...previous,
    [incoming.truckId]: {
      ...existing,
      gps: gpsIsNewer ? incoming.gps : existing.gps,
      gpsTimestamp: gpsIsNewer
        ? incoming.gpsTimestamp
        : existing.gpsTimestamp,
      obd: obdIsNewer ? incoming.obd : existing.obd,
      obdTimestamp: obdIsNewer
        ? incoming.obdTimestamp
        : existing.obdTimestamp,
    },
  };
}

function MapController({ truck }: { truck: Truck | undefined }) {
  const map = useMap();
  const lastFlownTruckId = useRef<string | null>(null);

  useEffect(() => {
    if (!truck) {
      lastFlownTruckId.current = null;
      return;
    }

    if (!truck.gps) return;

    // Move the map only when a different truck is selected.
    if (lastFlownTruckId.current === truck.truckId) return;

    map.flyTo(
      [truck.gps.latitude, truck.gps.longitude],
      13,
      { duration: 0.8 }
    );

    lastFlownTruckId.current = truck.truckId;
  }, [
    truck?.truckId,
    truck?.gps?.latitude,
    truck?.gps?.longitude,
    map,
  ]);

  return null;
}

function App() {
  const [trucks, setTrucks] = useState<Record<string, Truck>>({});
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedTruckId, setSelectedTruckId] = useState<string | null>(null);
  const [connectionStatus, setConnectionStatus] = useState<
  "connecting" | "connected" | "disconnected"
>("connecting");

  const [isLoading, setIsLoading] = useState(true);
  const [apiError, setApiError] = useState("");
  
const loadTrucks = async () => {
    setIsLoading(true);
    setApiError("");

    try {
      let cursor = "0";
      let hasMore = true;
      const truckData: Truck[] = [];

      while (hasMore) {
        const response = await fetch(
          `http://localhost:3000/api/trucks?limit=100&cursor=${cursor}`
        );

        if (!response.ok) {
          throw new Error("Could not load fleet data.");
        }

        const page: {
          trucks: Truck[];
          nextCursor: string;
          hasMore: boolean;
        } = await response.json();

        truckData.push(...page.trucks);

        cursor = page.nextCursor;
        hasMore = page.hasMore;
      }

      setTrucks((previous) => {
        const now = Date.now();

        return truckData.reduce(
          (current, truck) =>
            mergeTruckUpdate(current, {
              ...truck,
              lastSeen: now,
            }),
          previous
        );
      });
    } catch (error) {
      console.error("Fleet API error:", error);
      setApiError("Unable to load trucks. Check your backend connection.");
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadTrucks();

    // WebSocket connection with automatic reconnection.
    let socket: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout>;
    let shouldReconnect = true;

    const connectWebSocket = () => {
      setConnectionStatus("connecting");

      socket = new WebSocket("ws://localhost:3000");

      socket.onopen = () => {
        console.log("WebSocket connected");
        setConnectionStatus("connected");
      };

      socket.onmessage = (event) => {
        const update = JSON.parse(event.data);

        setTrucks((previous) => {
          const existing = previous[update.truckId] || {
            truckId: update.truckId,
            gps: null,
            obd: null,
            gpsTimestamp: null,
            obdTimestamp: null,
          };

          const incoming: Truck = {
            ...existing,
            ...(update.type === "gps"
              ? {
                  gps: update.data,
                  gpsTimestamp: update.timestamp,
                }
              : {
                  obd: update.data,
                  obdTimestamp: update.timestamp,
                }),
          };

          return mergeTruckUpdate(previous, incoming);
        });
      };

      socket.onclose = () => {
        setConnectionStatus("disconnected");

        if (shouldReconnect) {
          console.log("WebSocket disconnected. Retrying in 3 seconds...");
          reconnectTimer = setTimeout(connectWebSocket, 3000);
        }
      };

      socket.onerror = (error) => {
        console.error("WebSocket error:", error);
        socket?.close();
      };
    };

    connectWebSocket();

    return () => {
      shouldReconnect = false;
      clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, []);

  // Refresh the dashboard so online/offline status updates every second.
  useEffect(() => {
    const interval = setInterval(() => {
      setTrucks((previous) => ({ ...previous }));
    }, 1000);

    return () => clearInterval(interval);
  }, []);

  
    const truckList = Object.values(trucks);

    const isTruckOnline = (truck: Truck) => {
      const latestTimestamp = Math.max(
        Number(truck.gpsTimestamp ?? 0),
        Number(truck.obdTimestamp ?? 0)
      );

      return (
        latestTimestamp > 0 &&
        Date.now() / 1000 - latestTimestamp < 15
      );
    };

    const filteredTrucks = truckList
      .filter((truck) =>
        truck.truckId.toLowerCase().includes(searchTerm.toLowerCase())
      )
      .sort(
        (a, b) => Number(isTruckOnline(b)) - Number(isTruckOnline(a))
      );

    const firstTruck = truckList.find((truck) => truck.gps);

    const onlineCount = truckList.filter(isTruckOnline).length;

    const offlineCount = truckList.length - onlineCount;


  return (
    <div className="dashboard">
      <header>
        <h1>FleetOps</h1>

        <div className="header-status">
          <span
            className={`connection-indicator ${connectionStatus}`}
          >
            <span className="connection-dot"></span>

            {connectionStatus === "connected"
              ? "Backend Connected"
              : connectionStatus === "connecting"
              ? "Connecting..."
              : "Backend Disconnected"}
          </span>

          <span>Live Fleet Monitoring</span>
        </div>
      </header>

      <main>
        {/* Summary cards stay above the map and table. */}
        <section className="fleet-summary">
          <div className="summary-card">
            <h3>Total Trucks</h3>
            <p>{truckList.length}</p>
          </div>

          <div className="summary-card">
            <h3>Online</h3>
            <p>{onlineCount}</p>
          </div>

          <div className="summary-card">
            <h3>Offline</h3>
            <p>{offlineCount}</p>
          </div>
        </section>

        {/* Map and truck table sit side by side. */}
        <div className="dashboard-content">
          <section className="map-section">
            <h2>Live Truck Locations</h2>

            <MapContainer
              center={[
                firstTruck?.gps?.latitude ?? 19.076,
                firstTruck?.gps?.longitude ?? 72.8777,
              ]}
              zoom={11}
              className="map"
            >
              <MapController
                truck={
                  selectedTruckId
                    ? trucks[selectedTruckId]
                    : undefined
                }
              />

              <TileLayer
                attribution="&copy; OpenStreetMap contributors"
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />

              {truckList.map((truck) =>
                truck.gps ? (
                  <Marker
                    key={truck.truckId}
                    position={[
                      truck.gps.latitude,
                      truck.gps.longitude,
                    ]}
                    icon={
                      selectedTruckId === truck.truckId
                        ? selectedTruckIcon
                        : normalTruckIcon
                    }
                  >
                    <Popup>
                      <strong>{truck.truckId}</strong>
                      <p>Speed: {truck.gps.speed.toFixed(1)} km/h</p>
                      <p>
                        Latitude: {truck.gps.latitude.toFixed(6)}
                      </p>
                      <p>
                        Longitude: {truck.gps.longitude.toFixed(6)}
                      </p>
                    </Popup>
                  </Marker>
                ) : null
              )}
            </MapContainer>
          </section>

          <section className="truck-section">
            <div className="table-header">
              <h2>Fleet Overview ({filteredTrucks.length})</h2>

              <input
                type="text"
                placeholder="Search truck ID..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="search-input"
              />
            </div>

            
              {isLoading && (
                  <p className="loading-message">Loading fleet data...</p>
                )}

                {apiError && (
                  <div className="api-error">
                    <p>{apiError}</p>
                    <button onClick={loadTrucks}>Retry</button>
                  </div>
                )}

            <div className="table-container">
              <table className="fleet-table">
                <thead>
                  <tr>
                    <th>Truck ID</th>
                    <th>Status</th>
                    <th>Latitude</th>
                    <th>Longitude</th>
                    <th>Speed</th>
                    <th>Fuel</th>
                    <th>Engine RPM</th>
                    <th>Temperature</th>
                  </tr>
                </thead>

                
                <tbody>
                  {filteredTrucks.map((truck) => {
                    const latestTimestamp = Math.max(
                      Number(truck.gpsTimestamp ?? 0),
                      Number(truck.obdTimestamp ?? 0)
                    );

                    const isOnline =
                      latestTimestamp > 0 &&
                      Date.now() / 1000 - latestTimestamp < 15;

                    const isSelected = selectedTruckId === truck.truckId;

                    return (
                      <Fragment key={truck.truckId}>
                        <tr
                          onClick={() =>
                            setSelectedTruckId(
                              isSelected ? null : truck.truckId
                            )
                          }
                          className={isSelected ? "selected-row" : ""}
                          style={{ cursor: "pointer" }}
                        >
                          <td className="truck-id">{truck.truckId}</td>

                          <td>
                            <span
                              className={
                                isOnline ? "status-online" : "status-offline"
                              }
                            >
                              {isOnline ? "Online" : "Offline"}
                            </span>
                          </td>

                          <td>{truck.gps?.latitude.toFixed(6) ?? "—"}</td>
                          <td>{truck.gps?.longitude.toFixed(6) ?? "—"}</td>

                          <td>
                            {truck.gps
                              ? `${truck.gps.speed.toFixed(1)} km/h`
                              : "—"}
                          </td>

                          <td>
                            {truck.obd
                              ? `${truck.obd.fuelLevel.toFixed(1)}%`
                              : "—"}
                          </td>

                          <td>{truck.obd?.engineRPM ?? "—"}</td>

                          <td>
                            {truck.obd
                              ? `${truck.obd.engineTemperature.toFixed(1)} °C`
                              : "—"}
                          </td>
                        </tr>

                        {isSelected && (
                          <tr>
                            <td colSpan={8}>
                              <section className="truck-details">
                                <h2>Truck Details: {truck.truckId}</h2>

                                <h3>GPS Information</h3>
                                <p>
                                  Latitude: {truck.gps?.latitude.toFixed(6) ?? "—"}
                                </p>
                                <p>
                                  Longitude: {truck.gps?.longitude.toFixed(6) ?? "—"}
                                </p>
                                <p>
                                  Speed: {truck.gps?.speed.toFixed(1) ?? "—"} km/h
                                </p>

                                <h3>OBD Information</h3>
                                <p>Engine RPM: {truck.obd?.engineRPM ?? "—"}</p>
                                <p>
                                  Fuel: {truck.obd?.fuelLevel.toFixed(1) ?? "—"}%
                                </p>
                                <p>
                                  Temperature:{" "}
                                  {truck.obd?.engineTemperature.toFixed(1) ?? "—"} °C
                                </p>
                                <p>
                                  Battery:{" "}
                                  {truck.obd?.batteryVoltage.toFixed(2) ?? "—"} V
                                </p>

                                <button onClick={() => setSelectedTruckId(null)}>
                                  Close Details
                                </button>
                              </section>
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    );
                  })}
                </tbody>

              </table>

              {!isLoading && !apiError && filteredTrucks.length === 0 && (
                <p className="empty-state">No trucks found.</p>
              )}
            </div>
          </section>
        </div>
      </main>
    </div>
  );
}

export default App;
