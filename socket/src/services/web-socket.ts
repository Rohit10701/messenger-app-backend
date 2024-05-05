import WebSocket from "ws";
import http from "http";
import { messageSchema } from "../schema";
import { Message } from "../types";

class WebSocketServer {
  private wss: WebSocket.Server;

  constructor(server: http.Server) {
    this.wss = new WebSocket.Server({ server });

    this.wss.on("connection", (ws: WebSocket) => {
      console.log("A client connected.");
      // console.log(ws)
      ws.on("message", (data: string) => {
        console.log("Received message:", data);

        // valditor function and send fucntion
        this.handleMessage(data, ws);
      });

      ws.on("close", () => {
        console.log("Client disconnected.");
      });
    });
  }

  private handleMessage(data: string, ws: WebSocket) {
    try {
      const validaterResponse = messageSchema.safeParse(JSON.parse(data));
      if(validaterResponse?.error){
        throw Error("Couldn't Parsed the Message")
      }
      this.processMessage(validaterResponse.data);
    } catch (error) {
      console.error("Error parsing or validating message:", error);
    }
  }

  private processMessage(message: Message) {
    this.saveMessageToDB()

    // get the info of socket for the user from db
    this.ifUserOnline()

    // send it to the kafka
    this.sendMessageToKafka()
  }

  //   function for finding the user from user socket db
  private findSocketForConnectedUser(userID: string): WebSocket | null {
    return null;
  }

  private sendMessageToKafka(message: Message) {
    return null;
  }
}

export default WebSocketServer;
