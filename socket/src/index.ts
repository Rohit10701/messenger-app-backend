import express from "express";
import http from "http";
import { supabase } from "./supabase";
import expressApp from "./utils/express-app";
import { PORT, SOCKET_PORT } from "./config";
import WebSocketServer from "./services/web-socket";

const startServer = async () => {
  const app = express();
  const server = http.createServer(app);
  
  supabase();
  await expressApp(app);
  new WebSocketServer(server);

  server
    .listen(SOCKET_PORT, () => {
      console.log(`socket server listening to port ${SOCKET_PORT}`);
    })
    .on("error", (err) => {
      console.log(err);
      process.exit();
    });
  
  app
    .listen(PORT, () => {
      console.log(`listening to port ${PORT}`);
    })
    .on("error", (err) => {
      console.log(err);
      process.exit();
    });
};

startServer();
